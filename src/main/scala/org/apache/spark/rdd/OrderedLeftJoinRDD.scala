package org.apache.spark.rdd

import org.apache.spark._
import org.broadinstitute.hail.Utils._

import scala.reflect.ClassTag

class OrderedLeftJoinRDD[T, K, V1, V2](left: OrderedRDD[T, K, V1], right: OrderedRDD[T, K, V2])
  (implicit tOrd: Ordering[T], kOrd: Ordering[K], tct: ClassTag[T],
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
      val lastRightPartition = OrderedDependency.getDependencies(leftP, rightP)(split.index)._2

      val rightMatcher = new SkippingLookup[T, K, V2](right, leftP.projectKey, lastRightPartition, context)
      leftIterator.map { case (k, v) => (k, (v, rightMatcher.findMatch(k))) }
    }
  }
}

class SkippingLookup[T, K, V](rdd: OrderedRDD[T, K, V], projectKey: (K) => T, lastPartition: Int, context: TaskContext)
  (implicit tOrd: Ordering[T], kOrd: Ordering[K]) {
  val p = rdd.orderedPartitioner
  var partition = -1
  var it: Iterator[(K, V)] = Iterator[(K, V)]()
  var buffer: Option[(K, V)] = None
  var partitionMaxT: Option[T] = None

  def initialize(k: K) {
    partition = p.getPartition(k)
    it = rdd.iterator(rdd.partitions(partition), context)

    while (it.isEmpty && partition < lastPartition) {
      partition += 1
      it = rdd.iterator(rdd.partitions(partition), context)
    }
    if (partition < lastPartition)
      partitionMaxT = Some(p.rangeBounds(partition))
    if (it.nonEmpty) {
      buffer = Some(it.next())
    }
  }

  def advanceTo(k: K) {
    val t = projectKey(k)
    if (partitionMaxT.exists(tOrd.gt(t, _))) {
      while (partition < lastPartition && partitionMaxT.exists(tOrd.gt(t, _))) {
        partition += 1
        if (partition < lastPartition)
          partitionMaxT = Some(p.rangeBounds(partition))
      }
      it = rdd.iterator(rdd.partitions(partition), context)
      if (it.hasNext)
        buffer = Some(it.next())
      else
        buffer = None
    }

    if (buffer.exists { case (bufferK, _) => kOrd.lt(bufferK, k) }) {
      it = it.dropWhile { case (itK, _) => kOrd.lt(itK, k) }

      if (it.hasNext)
        buffer = Some(it.next())
      else
        buffer = None
    }
  }

  def findMatch(k: K): Option[V] = {
    if (partition < 0)
      initialize(k)

    advanceTo(k)

    buffer.flatMap { case (bufferK, bufferV) =>
      if (kOrd.equiv(k, bufferK))
        Some(bufferV)
      else {
        assert(kOrd.lt(k, bufferK))
        None
      }
    }
  }
}
