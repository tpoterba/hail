package is.hail.expr.types.physical

import is.hail.annotations.{Region, RegionValue}
import is.hail.asm4s.Code
import is.hail.expr.types.{TBoolean, TInterval, TStruct}
import is.hail.utils._

final case class PCanonicalInterval(pointType: PType) extends PInterval with PComplex {
  private val repType = TStruct(
    "start" -> pointType.virtualType,
    "end" -> pointType.virtualType,
    "includesStart" -> TBoolean(),
    "includesEnd" -> TBoolean())

  val repr = PCanonicalStruct(
    FastIndexedSeq(false -> pointType, false -> pointType, true -> PCanonicalBoolean, true -> PCanonicalBoolean),
    repType)

  def virtualType: TInterval = TInterval(pointType.virtualType)

  def startOffset(off: Code[Long]): Code[Long] = repr.fieldOffset(off, 0)

  def endOffset(off: Code[Long]): Code[Long] = repr.fieldOffset(off, 1)

  def loadStart(region: Region, off: Long): Long = repr.loadField(region, off, 0)

  def loadStart(region: Code[Region], off: Code[Long]): Code[Long] = repr.loadField(region, off, 0)

  def loadStart(rv: RegionValue): Long = loadStart(rv.region, rv.offset)

  def loadEnd(region: Region, off: Long): Long = repr.loadField(region, off, 1)

  def loadEnd(region: Code[Region], off: Code[Long]): Code[Long] = repr.loadField(region, off, 1)

  def loadEnd(rv: RegionValue): Long = loadEnd(rv.region, rv.offset)

  def startDefined(region: Region, off: Long): Boolean = repr.isFieldDefined(region, off, 0)

  def endDefined(region: Region, off: Long): Boolean = repr.isFieldDefined(region, off, 1)

  def includesStart(region: Region, off: Long): Boolean = PCanonicalBoolean.load(region, repr.loadField(region, off, 2))

  def includesEnd(region: Region, off: Long): Boolean = PCanonicalBoolean.load(region, repr.loadField(region, off, 3))

  def startDefined(region: Code[Region], off: Code[Long]): Code[Boolean] = repr.isFieldDefined(region, off, 0)

  def endDefined(region: Code[Region], off: Code[Long]): Code[Boolean] = repr.isFieldDefined(region, off, 1)

  def includeStart(region: Code[Region], off: Code[Long]): Code[Boolean] =
    PCanonicalBoolean.load(region, repr.loadField(region, off, 2))

  def includeEnd(region: Code[Region], off: Code[Long]): Code[Boolean] =
    PCanonicalBoolean.load(region, repr.loadField(region, off, 3))
}
