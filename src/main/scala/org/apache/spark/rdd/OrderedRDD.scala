package org.apache.spark.rdd

import org.apache.spark._
import org.broadinstitute.hail.Utils._

import scala.reflect.ClassTag

object OrderedRDD {
  def apply[K: ClassTag, V: ClassTag, T: ClassTag](rdd: RDD[(K, V)])(implicit ordering: Ordering[K], ev: (K) => T): OrderedRDD[K, V, T] = {
    rdd.partitioner match {
      case Some(p: OrderedPartitioner[T]) => new OrderedRDD(rdd, p)
      case _ =>
        val ranges = CalculateKeyRanges(rdd.keys.map(ev))
        val partitioner = OrderedPartitioner[T](ranges)
        new OrderedRDD(rdd.repartitionAndSortWithinPartitions(partitioner), partitioner)
    }
  }
}

class OrderedRDD[K, V, T](rdd: RDD[(K, V)], p: OrderedPartitioner[T])
  (implicit ordering: Ordering[K], ev: (K) => T) extends RDD[(K, V)](rdd) {

  def orderedPartitioner = p

  override val partitioner: Option[Partitioner] = Some(p)

  def getPartitions: Array[Partition] = rdd.partitions

  def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = rdd.compute(split, context)

  override def getPreferredLocations(split: Partition): Seq[String] = rdd.preferredLocations(split)
}

case class OneDependency[T](rdd: RDD[T]) extends Dependency[T]

case class OrderedJoinPartition(index: Int) extends Partition

object RangeDependency {
  def getDependencies[K](p1: OrderedPartitioner[K], p2: OrderedPartitioner[K])(partitionId: Int): Seq[Int] = {

    val lastPartition = if (partitionId == p1.rangeBounds.length)
      p2.numPartitions - 1
    else
      p2.getPartition(p1.rangeBounds(partitionId))

    if (partitionId == 0)
      0 to lastPartition
    else {
      val startPartition = p2.getPartition(p1.rangeBounds(partitionId - 1))
      startPartition to lastPartition
    }
  }
}

class RangeDependency[K, V, T](p1: OrderedPartitioner[K], p2: OrderedPartitioner[K], rdd: RDD[(T, V)]) extends NarrowDependency[(T, V)](rdd) {
  override def getParents(partitionId: Int): Seq[Int] = RangeDependency.getDependencies(p1, p2)(partitionId)
}

object OrderedRDDLeftJoin {
  def apply[K, V1, V2, T](rdd1: RDD[(K, V1)], rdd2: RDD[(K, V2)])
    (implicit ordering: Ordering[K], tct: ClassTag[K],
      vct1: ClassTag[V1], vct2: ClassTag[V2], ev: (K) => T): OrderedRDDLeftJoin[K, V1, V2, T] = {

    new OrderedRDDLeftJoin(OrderedRDD(rdd1), OrderedRDD(rdd2))
  }
}

class OrderedRDDLeftJoin[K, V1, V2, T](rdd1: OrderedRDD[K, V1, T], rdd2: OrderedRDD[K, V2, T])
  (implicit ordering: Ordering[K], ev1: (K) => T)
  extends RDD[(K, (V1, Option[V2]))](rdd1.sparkContext,
    Seq(new OneToOneDependency(rdd1), new RangeDependency(rdd1.orderedPartitioner, rdd2.orderedPartitioner, rdd2)): Seq[Dependency[_]]) {

  override val partitioner: Option[Partitioner] = rdd1.partitioner

  lazy val p1 = rdd1.orderedPartitioner
  lazy val p2 = rdd2.orderedPartitioner

  val localPartitions = rdd1.getPartitions.indices.map(OrderedJoinPartition).map(_.asInstanceOf[Partition]).toArray

  def getPartitions: Array[Partition] = localPartitions

  //  override def getPartitions: Array[Partition] = {
  //    val numParts = rdd1.partitions.length
  //    Array.tabulate[Partition](numParts) { i =>
  //
  //      val prefs = rdds.map(rdd => rdd.preferredLocations(rdd.partitions(i)))
  //      // Check whether there are any hosts that match all RDDs; otherwise return the union
  //      val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))
  //      val locs = if (!exactMatchLocations.isEmpty) exactMatchLocations else prefs.flatten.distinct
  //      new ZippedPartitionsPartition(i, rdds, locs)
  //    }
  //  }

  override def getPreferredLocations(split: Partition): Seq[String] = rdd1.preferredLocations(split)

  def compute(split: Partition, context: TaskContext): Iterator[(K, (V1, Option[V2]))] = {
    val left = rdd1.compute(split, context)
    val right = RangeDependency.getDependencies(p1, p2)(split.index)
      .iterator
      .flatMap(i => rdd2.compute(rdd2.partitions(i), context))

    left.sortedLeftJoinDistinct(right)
  }
}
