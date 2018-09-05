package is.hail.expr.types.physical

import is.hail.annotations.Region
import is.hail.asm4s.{Code, Settable, const, _}
import is.hail.expr.types.TContainer
import is.hail.utils._

object PCanonicalArray {
  def roundUpAlignment(offset: Long, alignment: Long): Long = {
    assert(alignment > 0)
    assert((alignment & (alignment - 1)) == 0) // power of 2
    (offset + (alignment - 1)) & ~(alignment - 1)
  }

  def roundUpAlignment(offset: Code[Long], alignment: Long): Code[Long] = {
    assert(alignment > 0)
    assert((alignment & (alignment - 1)) == 0) // power of 2
    (offset + (alignment - 1)) & ~(alignment - 1)
  }
}

final case class PCanonicalArray(elementType: PType, virtualType: TContainer, elementsRequired: Boolean) extends PConstructableArray {
  assert(elementType.virtualType == virtualType.elementType)
  val contentsAlignment: Long = elementType.alignment.max(4)
  val elementByteSize: Long = PCanonicalArray.arrayElementSize(elementType)

  def clearMissingBits(region: Region, aoff: Long, length: Int) {
    if (elementsRequired)
      return
    val nMissingBytes = (length + 7) / 8
    var i = 0
    while (i < nMissingBytes) {
      region.storeByte(aoff + 4 + i, 0)
      i += 1
    }
  }

  def allocate(region: Region, length: Int): Long = {
    region.allocate(contentsAlignment, contentsByteSize(length))
  }

  def initialize(region: Region, aoff: Long, length: Int) {
    region.storeInt(aoff, length)
    clearMissingBits(region, aoff, length)
  }

  def initialize(region: Code[Region], aoff: Code[Long], length: Code[Int], a: Settable[Int]): Code[Unit] = {
    var c = region.storeInt(aoff, length)
    if (elementsRequired)
      return c
    Code(
      c,
      a.store((length + 7) >>> 3),
      Code.whileLoop(a > 0,
        Code(
          a.store(a - 1),
          region.storeByte(aoff + 4L + a.toL, const(0))
        )
      )
    )
  }

  def setElementMissing(region: Region, aoff: Long, i: Int) {
    assert(!elementsRequired)
    region.setBit(aoff + 4, i)
  }

  def setElementMissing(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Unit] = {
    assert(!elementsRequired)
    region.setBit(aoff + 4L, i.toL)
  }

  def loadLength(region: Region, aoff: Long): Int =
    region.loadInt(aoff)

  def loadLength(region: Code[Region], aoff: Code[Long]): Code[Int] =
    region.loadInt(aoff)

  def isElementDefined(region: Region, aoff: Long, i: Int): Boolean =
    elementsRequired || !region.loadBit(aoff + 4, i)

  def isElementDefined(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Boolean] =
    if (elementsRequired)
      true
    else
      !region.loadBit(aoff + 4, i.toL)

  def loadElement(region: Region, aoff: Long, length: Int, i: Int): Long = {
    val off = elementOffset(aoff, length, i)
    if (elementType.isPointer)
    elementType match {
      case p: PPointer => p.dereference(region, off)
      case _ => off
    }
  }

  def loadElement(region: Code[Region], aoff: Code[Long], length: Code[Int], i: Code[Int]): Code[Long] = {
    val off = elementOffset(aoff, length, i)
    elementType match {
      case p: PPointer => p.dereference(region, off)
      case _ => off
    }
  }

  def loadElement(region: Region, aoff: Long, i: Int): Long =
    loadElement(region, aoff, region.loadInt(aoff), i)

  def loadElement(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Long] =
    loadElement(region, aoff, region.loadInt(aoff), i)

  def elementsOffset(length: Int): Long = {
    if (elementsRequired)
      PCanonicalArray.roundUpAlignment(4, elementType.alignment)
    else
      PCanonicalArray.roundUpAlignment(4 + ((length + 7) >>> 3), elementType.alignment)
  }

  def elementsOffset(length: Code[Int]): Code[Long] = {
    if (elementsRequired)
      PCanonicalArray.roundUpAlignment(4, elementType.alignment)
    else
      PCanonicalArray.roundUpAlignment(((length.toL + 7L) >>> 3) + 4L, elementType.alignment)
  }

  def elementOffset(aoff: Long, length: Int, i: Int): Long =
    aoff + elementsOffset(length) + i * elementByteSize

  def elementOffsetInRegion(region: Region, aoff: Long, i: Int): Long =
    elementOffset(aoff, loadLength(region, aoff), i)

  def elementOffset(aoff: Code[Long], length: Code[Int], i: Code[Int]): Code[Long] =
    aoff + elementsOffset(length) + i.toL * const(elementByteSize)

  def elementOffsetInRegion(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Long] =
    elementOffset(aoff, loadLength(region, aoff), i)

  def contentsByteSize(length: Int): Long =
    elementsOffset(length) + length * elementByteSize

  def contentsByteSize(length: Code[Int]): Code[Long] = {
    elementsOffset(length) + length.toL * elementByteSize
  }

}
