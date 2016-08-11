package org.apache.spark.rdd

import org.apache.spark._
import org.broadinstitute.hail.Utils._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing._

object OrderedRDD {


  def apply[T, K, V](rdd: RDD[(K, V)], reducedRDD: Option[RDD[K]] = None)
    (implicit ev: (K) => T, tOrd: Ordering[T], kOrd: Ordering[K], tct: ClassTag[T], kct: ClassTag[K]): OrderedRDD[T, K, V] = {

    def fromPartitionSummaries(arr: Array[(Int, Boolean, T, T)]): Option[OrderedRDD[T, K, V]] = {
      val allKSorted = arr.forall(_._2)

      val minMax = arr.map(x => (x._3, x._4))
      val sortedBetweenPartitions = minMax.tail.map(_._1).zip(minMax.map(_._2))
        .forall { case (nextMin, lastMax) => tOrd.gt(nextMin, lastMax) }

      if (sortedBetweenPartitions) {
        val ab = mutable.ArrayBuilder.make[T]()
        minMax.take(minMax.length - 1).foreach { case (_, max) => ab += max }

        val partitioner = OrderedPartitioner[T, K](ab.result())
        assert(partitioner.numPartitions == rdd.partitions.length)

        if (allKSorted) {
          info("Coerced key-sorted RDD")
          Some(new OrderedRDD[T, K, V](rdd, partitioner))
        } else {
          val sortedRDD = rdd.mapPartitionsWithIndex { case (i, it) =>
            if (arr(i)._2)
              it
            else it.localKeySort[T]
          }
          info("Coerced partition-key-sorted RDD")
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
        var lastT = ev(firstK)

        while (it.hasNext && continue) {
          val k = it.next()
          val t = ev(k)
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
        else Iterator(Some((i, sortedK, ev(firstK), ev(lastK))))
      }
    }

    def fromShuffle(): OrderedRDD[T, K, V] = {
      info("ordering with network shuffle")
      val ranges: Array[T] = calculateKeyRanges[T](reducedRDD.map(_.map(k => ev(k)))
        .getOrElse(rdd.map { case (k, _) => ev(k) }))
      val partitioner = OrderedPartitioner[T, K](ranges)
      new OrderedRDD[T, K, V](new ShuffledRDD[K, V, V](rdd, partitioner).setKeyOrdering(kOrd), partitioner)
    }

    rdd match {
      case _: OrderedRDD[T, K, V] =>
        rdd.asInstanceOf[OrderedRDD[T, K, V]]
      case _: EmptyRDD[(K, V)] => new OrderedRDD(rdd, OrderedPartitioner(Array.empty[T]))
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
    * Lifted from org.apache.spark.RangePartitioner.scala in org.apache.spark
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

class OrderedRDD[T, K, V](rdd: RDD[(K, V)], p: OrderedPartitioner[T, K])
  (implicit ev: (K) => T, tOrd: Ordering[T], kOrd: Ordering[K], tct: ClassTag[T], kct: ClassTag[K]) extends RDD[(K, V)](rdd) {

  def orderedPartitioner = p

  override val partitioner: Option[Partitioner] = Some(p)

  def getPartitions: Array[Partition] = rdd.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = rdd.iterator(split, context)

  override def getPreferredLocations(split: Partition): Seq[String] = rdd.preferredLocations(split)
}
