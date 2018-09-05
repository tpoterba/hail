package is.hail.expr.types.physical

import is.hail.annotations.Region
import is.hail.asm4s.{Code, const, _}
import is.hail.expr.types.TBaseStruct
import is.hail.utils._


final case class PCanonicalStruct(
  fields: IndexedSeq[(Boolean, PType)],
  virtualType: TBaseStruct) extends PConstructableStruct {
  assert(fields.length == virtualType.size)

  val size = fields.length
  private val (nMissing, missingIdx) = {
    val a = Array.fill[Int](fields.length)(-1)
    var i = 0
    fields.zipWithIndex.foreach { case ((req, _), idx) =>
      if (!req) {
        a(idx) = i
        i += 1
      }
    }
    i -> a
  }

  val fieldRequired = fields.map(_._1).toArray
  val types = fields.map(_._2).toArray
  private val nMissingBytes = (nMissing + 7) >>> 3
  val byteOffsets = new Array[Long](size)
  val byteSize: Long = PCanonicalStruct.getByteSizeAndOffsets(types, nMissingBytes, byteOffsets)
  val alignment: Long = PCanonicalStruct.alignment(types)

  def fieldType(field: Int): PType = types(field)

  def clearMissingBits(region: Region, off: Long) {
    var i = 0
    while (i < nMissingBytes) {
      region.storeByte(off + i, 0)
      i += 1
    }
  }

  def clearMissingBits(region: Code[Region], off: Code[Long]): Code[Unit] = {
    var c: Code[Unit] = Code._empty
    var i = 0
    while (i < nMissingBytes) {
      c = Code(c, region.storeByte(off + i.toLong, const(0)))
      i += 1
    }
    c
  }

  def allocate(region: Region): Long = {
    region.allocate(alignment, byteSize)
  }

  def isFieldDefined(region: Region, offset: Long, fieldIdx: Int): Boolean =
    fieldRequired(fieldIdx) || !region.loadBit(offset, missingIdx(fieldIdx))

  def isFieldMissing(region: Code[Region], offset: Code[Long], fieldIdx: Int): Code[Boolean] =
    if (fieldRequired(fieldIdx))
      false
    else
      region.loadBit(offset, missingIdx(fieldIdx))

  def setFieldMissing(region: Region, offset: Long, fieldIdx: Int) {
    assert(!fieldRequired(fieldIdx))
    region.setBit(offset, missingIdx(fieldIdx))
  }

  def setFieldMissing(region: Code[Region], offset: Code[Long], fieldIdx: Int): Code[Unit] = {
    assert(!fieldRequired(fieldIdx))
    region.setBit(offset, missingIdx(fieldIdx))
  }

  def fieldOffset(offset: Long, fieldIdx: Int): Long =
    offset + byteOffsets(fieldIdx)

  def fieldOffset(offset: Code[Long], fieldIdx: Int): Code[Long] =
    offset + byteOffsets(fieldIdx)

  def loadField(region: Region, offset: Long, fieldIdx: Int): Long = {
    val off = fieldOffset(offset, fieldIdx)
    types(fieldIdx) match {
      case p: PPointer => p.dereference(region, off)
      case _ => off
    }
  }

  def loadField(region: Code[Region], offset: Code[Long], fieldIdx: Int): Code[Long] = {
    val off = fieldOffset(offset, fieldIdx)
    types(fieldIdx) match {
      case p: PPointer => p.dereference(region, off)
      case _ => off
    }
  }
}

object PCanonicalStruct {
  def getByteSizeAndOffsets(types: Array[PType], nMissingBytes: Long, byteOffsets: Array[Long]): Long = {
    assert(byteOffsets.length == types.length)
    val bp = new BytePacker()

    var offset: Long = nMissingBytes
    types.zipWithIndex.foreach { case (t, i) =>
      val fSize = t.byteSize
      val fAlignment = t.alignment

      bp.getSpace(fSize, fAlignment) match {
        case Some(start) =>
          byteOffsets(i) = start
        case None =>
          val mod = offset % fAlignment
          if (mod != 0) {
            val shift = fAlignment - mod
            bp.insertSpace(shift, offset)
            offset += (fAlignment - mod)
          }
          byteOffsets(i) = offset
          offset += fSize
      }
    }
    offset
  }

  def alignment(types: Array[PType]): Long = {
    if (types.isEmpty)
      1
    else
      types.map(_.alignment).max
  }
}