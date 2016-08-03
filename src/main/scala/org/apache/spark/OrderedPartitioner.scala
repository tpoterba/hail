package org.apache.spark

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{CollectionsUtils, Utils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing._

/**
  * A [[org.apache.spark.Partitioner]] that partitions sortable records by range into roughly
  * equal ranges. The ranges are determined by sampling the content of the RDD passed in.
  *
  * Note that the actual number of partitions created by the OrderedPartitioner might not be the same
  * as the `partitions` parameter, in the case where the number of sampled records is less than
  * the value of `partitions`.
  */
case class OrderedPartitioner[T: Ordering : ClassTag, K: Ordering : ClassTag](
  rangeBounds: Array[T],
  f: (K) => T,
  private var ascending: Boolean = true)
  extends Partitioner {

  def write(out: ObjectOutputStream) {
    out.writeBoolean(ascending)
    out.writeObject(rangeBounds)
  }

  var ordering = implicitly[Ordering[T]]

  def numPartitions: Int = rangeBounds.length + 1

  var binarySearch: ((Array[T], T) => Int) = CollectionsUtils.makeBinarySearch[T]

  def getPartition(key: Any): Int = {
    val k = f(key.asInstanceOf[K])
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition - 1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: OrderedPartitioner[_, _] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }
}

object OrderedPartitioner {
  def read[T: Ordering : ClassTag, K : Ordering : ClassTag](in: ObjectInputStream)(implicit ev: (K) => T): OrderedPartitioner[T, K] = {
    val ascending = in.readBoolean()
    val rangeBounds = in.readObject().asInstanceOf[Array[T]]
    OrderedPartitioner(rangeBounds, ev, ascending)
  }
}
