package org.apache.spark.rdd

import org.apache.spark._
import org.broadinstitute.hail.Utils._

import scala.reflect.ClassTag

class OrderedLeftJoinRDD[T, K, V1, V2](left: OrderedRDD[T, K, V1], right: OrderedRDD[T, K, V2])
  (implicit ev: (K) => T, tOrd: Ordering[T], kOrd: Ordering[K], tct: ClassTag[T],
    kct: ClassTag[K]) extends RDD[(K, (V1, Option[V2]))](left.sparkContext, Seq(new OneToOneDependency(left),
  new OrderedDependency(left.orderedPartitioner, right.orderedPartitioner, right)): Seq[Dependency[_]]) {

  override val partitioner: Option[Partitioner] = left.partitioner

  val rightPartitions = right.partitions // compute and store -- getPartitions called on a worker throw a NullPointerException

  def getPartitions: Array[Partition] = left.partitions

  override def getPreferredLocations(split: Partition): Seq[String] = left.preferredLocations(split)

  override def compute(split: Partition, context: TaskContext): Iterator[(K, (V1, Option[V2]))] = {

    val leftP = left.orderedPartitioner
    val rightP = right.orderedPartitioner

    val leftIterator = left.iterator(split, context)

    if (leftIterator.isEmpty)
      Iterator.empty
    else {
      val first = leftIterator.next()
      val rightStart = rightP.getPartition(first._1)
      val rightIterator = (rightStart to OrderedDependency.getDependencies(leftP, rightP)(split.index)._2)
        .iterator
        .flatMap(i => {
          right.iterator(rightPartitions(i), context)
        })

      (Iterator(first) ++ leftIterator).sortedLeftJoinDistinct(rightIterator)
    }
  }
}
