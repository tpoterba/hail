package org.apache.spark.rdd

import org.apache.spark._
import org.broadinstitute.hail.Utils._

import scala.reflect.ClassTag

class OrderedLeftJoinTransformedRDD[T, K1, K2, V1, V2](left: OrderedRDD[T, K1, V1], right: OrderedRDD[T, K2, V2])
  (implicit ev: (K1) => T, ev2: (K2) => T, tOrd: Ordering[T], kOrd1: Ordering[K1],
    kOrd2: Ordering[K2], tct: ClassTag[T], kct1: ClassTag[K1],
    kct2: ClassTag[K2]) extends RDD[(K1, (V1, Option[V2]))](left.sparkContext, Seq(new OneToOneDependency(left),
  new OrderedDependency(left.orderedPartitioner, right.orderedPartitioner, right)): Seq[Dependency[_]]) {

  override val partitioner: Option[Partitioner] = left.partitioner

  val rightPartitions = right.partitions // compute and store -- getPartitions called on a worker throw a NullPointerExceptione

  def getPartitions: Array[Partition] = left.partitions

  override def getPreferredLocations(split: Partition): Seq[String] = left.preferredLocations(split)

  override def compute(split: Partition, context: TaskContext): Iterator[(K1, (V1, Option[V2]))] = {

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

      (Iterator(first) ++ leftIterator).sortedTransformedLeftJoinDistinct[T, K2, V2](rightIterator)
    }
  }
}


