package org.broadinstitute.hail.sparkextras

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, NarrowDependency, Partition, TaskContext}

import scala.reflect.ClassTag

case class AdjustedPartitionsRDDPartition[T](index: Int, parent: Partition, adj: Adjustment[T, Partition]) extends Partition

case class Adjustment[T, U](originalIndex: Int, localOp: Option[Iterator[T] => Iterator[T]],
  remoteOps: Option[(Seq[U], Iterator[T] => Iterator[T])]) {

  def map[V](f: U => V): Adjustment[T, V] = copy(remoteOps = remoteOps.map { case (it, g) => (it.map(f), g) })
}

class AdjustedPartitionsRDD[T](@transient var prev: RDD[T], adjustments: IndexedSeq[Adjustment[T, Int]])(implicit tct: ClassTag[T])
  extends RDD[T](prev.sparkContext, Nil) {
  require(adjustments.length <= prev.partitions.length,
    "invalid adjustments: size greater than previous partitions size")
  require(adjustments.forall(adj => adj.remoteOps.forall { case (it, _) =>
    it.nonEmpty && it.head == adj.originalIndex + 1 && it.zip(it.tail).forall { case (l, r) => l + 1 == r }
  }), "invalid adjustments: nonconsecutive span")

  override def getPartitions: Array[Partition] = {
    val parentPartitions = dependencies.head.rdd.asInstanceOf[RDD[T]].partitions
    Array.tabulate(adjustments.length) { i =>
      AdjustedPartitionsRDDPartition(i, parentPartitions(i), adjustments(i).map(parentPartitions))
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val parent = dependencies.head.rdd.asInstanceOf[RDD[T]]
    val adjPartition = split.asInstanceOf[AdjustedPartitionsRDDPartition[T]]
    val adj = adjPartition.adj

    val it = parent.compute(adjPartition.parent, context)
    val dropped: Iterator[T] = adj.localOp
      .map( f => f(it))
      .getOrElse(it)

    val taken: Iterator[T] = adj.remoteOps
      .map { case (additionalPartitions, f) =>
        additionalPartitions.iterator
          .flatMap { p => f(parent.compute(p, context)) }
      }.getOrElse(Iterator())

    dropped ++ taken
  }

  override def getDependencies: Seq[Dependency[_]] = Seq(new NarrowDependency[T](prev) {
    override def getParents(partitionId: Int): Seq[Int] = {
      val adj = adjustments(partitionId)
      adj.remoteOps.map { case (it, _) => Seq(adj.originalIndex) ++ it }
        .getOrElse(Seq(adj.originalIndex))
    }
  })

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val adjustedPartition = partition.asInstanceOf[AdjustedPartitionsRDDPartition[T]]
    prev.preferredLocations(adjustedPartition.parent) ++ adjustedPartition.adj.remoteOps.map { case (parents, _) =>
      parents.flatMap { p => prev.preferredLocations(p) }
    }.getOrElse(Seq())
  }
}
