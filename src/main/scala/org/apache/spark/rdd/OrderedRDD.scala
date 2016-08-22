package org.apache.spark.rdd

import org.apache.spark._
import org.broadinstitute.hail.Utils._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing._

object OrderedRDD {

  def empty[T, K, V](sc: SparkContext, projectKey: (K) => T)(implicit tOrd: Ordering[T], kOrd: Ordering[K], tct: ClassTag[T],
    kct: ClassTag[K]): OrderedRDD[T, K, V] = new OrderedRDD[T, K, V](sc.emptyRDD[(K, V)], OrderedPartitioner.empty[T, K](projectKey))

  def apply[T, K, V](rdd: RDD[(K, V)], projectKey: (K) => T, reducedRDD: Option[RDD[K]] = None)
    (implicit tOrd: Ordering[T], kOrd: Ordering[K], tct: ClassTag[T], kct: ClassTag[K]): OrderedRDD[T, K, V] = {

    def fromPartitionSummaries(arr: Array[(Int, Boolean, T, T)]): Option[OrderedRDD[T, K, V]] = {
      val allKSorted = arr.forall(_._2)

      val minMax = arr.map(x => (x._3, x._4))
      val sortedBetweenPartitions = minMax.tail.map(_._1).zip(minMax.map(_._2))
        .forall { case (nextMin, lastMax) => tOrd.gt(nextMin, lastMax) }

      if (sortedBetweenPartitions) {
        val ab = mutable.ArrayBuilder.make[T]()
        minMax.take(minMax.length - 1).foreach { case (_, max) => ab += max }

        val partitioner = OrderedPartitioner[T, K](ab.result(), projectKey)
        assert(partitioner.numPartitions == rdd.partitions.length)

        if (allKSorted) {
          info("Coerced sorted dataset")
          Some(new OrderedRDD[T, K, V](rdd, partitioner))
        } else {
          val sortedRDD = rdd.mapPartitionsWithIndex { case (i, it) =>
            if (arr(i)._2)
              it
            else it.localKeySort[T](projectKey)
          }
          info("Coerced almost-sorted dataset")
          Some(new OrderedRDD(sortedRDD, partitioner))
        }
      }
      else None
    }

    def verifySortedness(i: Int, it: Iterator[K]): Iterator[Option[(Int, Boolean, T, T)]] = {
      if (it.isEmpty)
        Iterator(None)
      else {
        var sortedT = true
        var sortedK = true
        var continue = true
        val firstK = it.next()
        var lastK = firstK
        var lastT = projectKey(firstK)

        while (it.hasNext && continue) {
          val k = it.next()
          val t = projectKey(k)
          if (tOrd.lt(t, lastT)) {
            sortedT = false
            continue = false
          } else if (kOrd.lt(k, lastK))
            sortedK = false

          lastK = k
          lastT = t
        }
        if (!sortedT)
          Iterator(None)
        else Iterator(Some((i, sortedK, projectKey(firstK), projectKey(lastK))))
      }
    }

    def fromShuffle(): OrderedRDD[T, K, V] = {
      info("Ordering unsorted dataset with network shuffle")
      val ranges: Array[T] = calculateKeyRanges[T](reducedRDD.map(_.map(k => projectKey(k)))
        .getOrElse(rdd.map { case (k, _) => projectKey(k) }))
      val partitioner = OrderedPartitioner[T, K](ranges, projectKey)
      new OrderedRDD[T, K, V](new ShuffledRDD[K, V, V](rdd, partitioner).setKeyOrdering(kOrd), partitioner)
    }

    rdd match {
      case _: OrderedRDD[T, K, V] =>
        rdd.asInstanceOf[OrderedRDD[T, K, V]]
      case _: EmptyRDD[(K, V)] => new OrderedRDD(rdd, OrderedPartitioner(Array.empty[T], projectKey))
      case _ =>
        rdd.partitioner match {
          case Some(p: OrderedPartitioner[T, K]) =>
            new OrderedRDD(rdd, p)
          case _ =>
            val result = reducedRDD.getOrElse(rdd.map(_._1))
              .mapPartitionsWithIndex(verifySortedness).collect()

            anyFailAllFail[Array, (Int, Boolean, T, T)](result)
              .map(_.sortBy(_._1))
              .flatMap(fromPartitionSummaries)
              .getOrElse(fromShuffle())
        }
    }
  }

  /**
    * Copied from:
    *   org.apache.spark.RangePartitioner
    *   version 1.5.0
    */
  def calculateKeyRanges[T](rdd: RDD[T])(implicit ord: Ordering[T], tct: ClassTag[T]): Array[T] = {
    // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
    val sampleSize = math.min(20.0 * rdd.partitions.length, 1e6)
    // Assume the input partitions are roughly balanced and over-sample a little bit.
    val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
    val (numItems, sketched) = RangePartitioner.sketch(rdd, sampleSizePerPartition)
    if (numItems == 0L) {
      Array.empty
    } else {
      // If a partition contains much more than the average number of items, we re-sample from it
      // to ensure that enough items are collected from that partition.
      val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
      val candidates = ArrayBuffer.empty[(T, Float)]
      val imbalancedPartitions = mutable.Set.empty[Int]
      sketched.foreach {
        case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
      }
      if (imbalancedPartitions.nonEmpty) {
        // Re-sample imbalanced partitions with the desired sampling probability.
        val imbalanced = new PartitionPruningRDD(rdd, imbalancedPartitions.contains)
        val seed = byteswap32(-rdd.id - 1)
        val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
        val weight = (1.0 / fraction).toFloat
        candidates ++= reSampled.map(x => (x, weight))
      }
      RangePartitioner.determineBounds(candidates, rdd.partitions.length)
    }
  }

}

class OrderedRDD[T, K, V](rdd: RDD[(K, V)],
  val orderedPartitioner: OrderedPartitioner[T, K])
  (implicit tOrd: Ordering[T], kOrd: Ordering[K], tct: ClassTag[T], kct: ClassTag[K]) extends RDD[(K, V)](rdd) {

  override val partitioner: Option[Partitioner] = Some(orderedPartitioner)

  private val cachedPartitions = rdd.partitions

  def getPartitions: Array[Partition] = cachedPartitions

  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = rdd.iterator(split, context)

  override def getPreferredLocations(split: Partition): Seq[String] = rdd.preferredLocations(split)

  def orderedLeftJoinDistinct[V2](other: OrderedRDD[T, K, V2]): RDD[(K, (V, Option[V2]))] =
    new OrderedLeftJoinRDD[T, K, V, V2](this, other)

  def mapMonotonic[K2, V2](f: (K, V) => (K2, V2), projectKey2: (K2) => T)(implicit k2Ord: Ordering[K2], k2ct: ClassTag[K2]): OrderedRDD[T, K2, V2] = {
    new OrderedRDD[T, K2, V2](
      rdd.mapPartitions(_.map(f.tupled)),
      orderedPartitioner.mapMonotonic(projectKey2))
  }
}
