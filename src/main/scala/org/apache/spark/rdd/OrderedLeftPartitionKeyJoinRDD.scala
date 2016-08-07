package org.apache.spark.rdd

import org.apache.spark._
import org.broadinstitute.hail.Utils._

import scala.reflect.ClassTag

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

  val r2Partitions = rdd2.partitions // compute and store -- getPartitions called on a worker throw a NullPointerException

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