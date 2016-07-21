package org.apache.spark

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.util.CollectionsUtils

import scala.reflect.ClassTag

case class OrderedPartitioner[T: Ordering : ClassTag, K: Ordering : ClassTag](
  rangeBounds: Array[T],
  private var ascending: Boolean = true)(implicit f: (K) => T)
  extends Partitioner {

  var ordering = implicitly[Ordering[T]]

  require(rangeBounds.isEmpty ||
    rangeBounds.zip(rangeBounds.tail).forall { case (left, right) => ordering.lt(left, right) })

  def write(out: ObjectOutputStream) {
    out.writeBoolean(ascending)
    out.writeObject(rangeBounds)
  }

  def numPartitions: Int = rangeBounds.length + 1

  var binarySearch: ((Array[T], T) => Int) = CollectionsUtils.makeBinarySearch[T]

  def getPartition(key: Any): Int = getPartitionT(f(key.asInstanceOf[K]))

  /**
    * Code mostly copied from:
    *   org.apache.spark.RangePartitioner.getPartition(key: Any)
    *   version 1.5.0
    **/
  def getPartitionT(key: T): Int = {

    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(key, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, key)
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
    case r: OrderedPartitioner[_, _] => r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ => false
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
  def empty[T, K](implicit ev: (K) => T, tOrd: Ordering[T], kOrd: Ordering[K], tct: ClassTag[T],
    kct: ClassTag[K]): OrderedPartitioner[T, K] = new OrderedPartitioner(Array.empty[T])

  def read[T, K](in: ObjectInputStream)(implicit ev: (K) => T, tOrd: Ordering[T], kOrd: Ordering[K], tct: ClassTag[T],
    kct: ClassTag[K]): OrderedPartitioner[T, K] = {
    val ascending = in.readBoolean()
    val rangeBounds = in.readObject().asInstanceOf[Array[T]]
    OrderedPartitioner(rangeBounds, ascending)
  }
}
