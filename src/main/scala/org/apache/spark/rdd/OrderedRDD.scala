package org.apache.spark.rdd

import org.apache.spark._
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.variant.Variant

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing._

case class SimplePartition(index: Int) extends Partition

object OrderedRDD {
  def apply[T, K, V](rdd: RDD[(K, V)])(implicit ev: (K) => T, tOrd: Ordering[T], kOrd: Ordering[K],
    tct: ClassTag[T], kct: ClassTag[K]): OrderedRDD[T, K, V] = {
    rdd.partitioner match {
      case Some(p: OrderedPartitioner[T, K]) =>
        println("found ordered partitioner")
        new OrderedRDD(rdd, p)
      case _ =>
        println("didn't find ordered partitioner")
        println(rdd.getClass.getName)
        println(s"in ORDD apply: ev = ${ ev(Variant("CHROM", 100, "A", "T").asInstanceOf[K]) }")

        val ranges = calculateKeyRanges[T](rdd.map { case (k, _) => ev(k) })
        val partitioner = OrderedPartitioner[T, K](ranges, ev)
        new OrderedRDD[T, K, V](new ShuffledRDD[K, V, V](rdd, partitioner).setKeyOrdering(kOrd), partitioner)
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
      sketched.foreach { case (idx, n, sample) =>
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

  println(s"constructed ordered rdd from parent ${ rdd.getClass.getName }")
  println(s"in ORDD: ev = ${ ev(Variant("CHROM", 100, "A", "T").asInstanceOf[K]) }")

  def orderedPartitioner = p

  override val partitioner: Option[Partitioner] = Some(p)

  def getPartitions: Array[Partition] = rdd.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = rdd.iterator(split, context)

  override def getPreferredLocations(split: Partition): Seq[String] = rdd.preferredLocations(split)
}

case class OneDependency[T](rdd: RDD[T]) extends Dependency[T]

case class OrderedJoinPartition(index: Int) extends Partition

object OrderedDependency {
  def getDependencies[T](p1: OrderedPartitioner[T, _], p2: OrderedPartitioner[T, _])(partitionId: Int): (Int, Int) = {

    val lastPartition = if (partitionId == p1.rangeBounds.length)
      p2.numPartitions - 1
    else
      p2.getPartitionT(p1.rangeBounds(partitionId))

    if (partitionId == 0)
      (0, lastPartition)
    else {
      val startPartition = p2.getPartitionT(p1.rangeBounds(partitionId - 1))
      (startPartition, lastPartition)
    }
  }
}

class OrderedDependency[T, K1, K2, V](p1: OrderedPartitioner[T, K1], p2: OrderedPartitioner[T, K2],
  rdd: RDD[(K2, V)]) extends NarrowDependency[(K2, V)](rdd) {
  override def getParents(partitionId: Int): Seq[Int] = {
    val (start, end) = OrderedDependency.getDependencies(p1, p2)(partitionId)
    start until end
  }
}

object OrderedLeftJoinRDD {
  def apply[T, K, V1, V2](rdd1: RDD[(K, V1)], rdd2: RDD[(K, V2)])
    (implicit ev: (K) => T, tOrd: Ordering[T], kOrd: Ordering[K], tct: ClassTag[T], kct: ClassTag[K]): OrderedLeftJoinRDD[T, K, V1, V2] = {

    new OrderedLeftJoinRDD(OrderedRDD[T, K, V1](rdd1), OrderedRDD[T, K, V2](rdd2))
  }
}

class OrderedLeftJoinRDD[T, K, V1, V2](rdd1: OrderedRDD[T, K, V1], rdd2: OrderedRDD[T, K, V2])
  (implicit ev: (K) => T, tOrd: Ordering[T], kOrd: Ordering[K], tct: ClassTag[T], kct: ClassTag[K])
  extends RDD[(K, (V1, Option[V2]))](rdd1.sparkContext,
    Seq(new OneToOneDependency(rdd1), new OrderedDependency(rdd1.orderedPartitioner, rdd2.orderedPartitioner, rdd2)): Seq[Dependency[_]]) {

  override val partitioner: Option[Partitioner] = rdd1.partitioner

  private def p1 = rdd1.orderedPartitioner

  private def p2 = rdd2.orderedPartitioner

  val r2Partitions = rdd2.partitions // FIXME wtf

  def getPartitions: Array[Partition] = rdd1.partitions

  override def getPreferredLocations(split: Partition): Seq[String] = rdd1.preferredLocations(split)

  override def compute(split: Partition, context: TaskContext): Iterator[(K, (V1, Option[V2]))] = {
    val left = rdd1.iterator(split, context)
    if (left.isEmpty)
      Iterator.empty
    else {
      val first = left.next()
      val rightStart = p2.getPartition(first._1)
      val right = (rightStart to OrderedDependency.getDependencies(p1, p2)(split.index)._2)
        .iterator
        .flatMap(i => {
          rdd2.iterator(r2Partitions(i), context)
        })
      (Iterator(first) ++ left).sortedLeftJoinDistinct(right)
    }
  }
}

object OrderedLeftPartitionKeyJoinRDD {
  def apply[T, K, V1, V2](rdd1: RDD[(K, V1)], rdd2: RDD[(T, V2)])
    (implicit ev: (K) => T, tOrd: Ordering[T], kOrd: Ordering[K], tct: ClassTag[T], kct: ClassTag[K]): OrderedLeftPartitionKeyJoinRDD[T, K, V1, V2] = {

    new OrderedLeftPartitionKeyJoinRDD(OrderedRDD[T, K, V1](rdd1), OrderedRDD[T, T, V2](rdd2))
  }
}

class OrderedLeftPartitionKeyJoinRDD[T, K, V1, V2](rdd1: OrderedRDD[T, K, V1],
  rdd2: OrderedRDD[T, T, V2])(implicit ev: (K) => T, tOrd: Ordering[T], kOrd: Ordering[K], tct: ClassTag[T],
  kct: ClassTag[K]) extends RDD[(K, (V1, Option[V2]))](rdd1.sparkContext, Seq(new OneToOneDependency(rdd1),
  new OrderedDependency(rdd1.orderedPartitioner, rdd2.orderedPartitioner, rdd2)): Seq[Dependency[_]]) {

  override val partitioner: Option[Partitioner] = rdd1.partitioner

  private def p1 = rdd1.orderedPartitioner

  private def p2 = rdd2.orderedPartitioner

  val r2Partitions = rdd2.partitions // FIXME wtf

  def getPartitions: Array[Partition] = rdd1.partitions

  override def getPreferredLocations(split: Partition): Seq[String] = rdd1.preferredLocations(split)

  override def compute(split: Partition, context: TaskContext): Iterator[(K, (V1, Option[V2]))] = {

    val left = rdd1.iterator(split, context)

    if (left.isEmpty)
      Iterator.empty
    else {
      val first = left.next()
      val rightStart = p2.getPartition(first._1)
      val right = (rightStart to OrderedDependency.getDependencies(p1, p2)(split.index)._2)
        .iterator
        .flatMap(i => {
          rdd2.iterator(r2Partitions(i), context)
        })

      (Iterator(first) ++ left).sortedTransformedLeftJoinDistinct(right)
    }
  }

}