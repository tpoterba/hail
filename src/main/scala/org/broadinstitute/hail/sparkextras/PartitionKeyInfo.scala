package org.broadinstitute.hail.sparkextras

import org.broadinstitute.hail.utils._
import scala.collection.mutable
import Ordering.Implicits._

case class SpanInfo[PK](sortedness: Int, min: PK, max: PK)

class SpanInfoBuilder[PK, K](init: K)(implicit kOk: OrderedKey[_, PK, K]) {

  import kOk._

  var sortedness = PartitionKeyInfo.KSORTED
  var minPK = project(init)
  var maxPK = minPK
  var prevK = init
  var prevPK = minPK

  def update(k: K): SpanInfoBuilder[PK, K] = {
    val pk = project(k)
    if (pk < prevPK)
      sortedness = PartitionKeyInfo.UNSORTED
    else if (k < prevK)
      sortedness = math.min(sortedness, PartitionKeyInfo.TSORTED)

    if (pk < minPK)
      minPK = pk
    if (pk > maxPK)
      maxPK = pk

    prevK = k
    prevPK = pk

    this
  }

  def result(): SpanInfo[PK] = SpanInfo(sortedness, minPK, maxPK)
}

class PartitionKeyInfoBuilder[MPK, PK, K](implicit kOk: OrderedKey[MPK, PK, K]) {
  import kOk._
  val m = mutable.Map.empty[MPK, (SpanInfoBuilder[PK, K])]

  var prev: K = _
  var totalSortedness = PartitionKeyInfo.KSORTED

  def update(k: K) {
    if (prev == null)
      prev = k
    else {

    }
    val pk = kOk.project(k)
    val mpk = kOk.projectPK(pk)

    m.updateValue(mpk, new SpanInfoBuilder[PK, K](k), b => b.update(k))
  }

  def result(index: Int): PartitionKeyInfo[MPK, PK] = {
    PartitionKeyInfo(index, m.map { case (mpk, pkb) => (mpk, pkb.result()) }.toMap)
  }
}

case class PartitionKeyInfo[MPK, PK](partIndex: Int, mpkMap: Map[MPK, SpanInfo[PK]])(implicit kOk: OrderedKey[MPK, PK, _]) {

  lazy val min: PK = {
    mpkMap.minBy(_._1)
      ._2
      .min
  }

  lazy val max: PK = {
    mpkMap.maxBy(_._1)
      ._2
      .max
  }

  def sortedness: Int = mpkMap.valuesIterator.map(_.sortedness).min
}

object PartitionKeyInfo {
  final val UNSORTED = 0
  final val TSORTED = 1
  final val KSORTED = 2

  def apply[MPK, PK, K](partIndex: Int, it: Iterator[K])
    (implicit kOk: OrderedKey[MPK, PK, K]): PartitionKeyInfo[MPK, PK] = {
    import kOk.kOrd
    import kOk.pkOrd

    assert(it.hasNext)

    val pkb = new PartitionKeyInfoBuilder[MPK, PK, K]
    it.foreach(pkb.update)

    pkb.result(partIndex)
  }
}