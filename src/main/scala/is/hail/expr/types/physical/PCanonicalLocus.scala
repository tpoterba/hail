package is.hail.expr.types.physical

import is.hail.annotations.Region
import is.hail.asm4s.Code
import is.hail.expr.types._
import is.hail.utils.FastIndexedSeq
import is.hail.variant.{RGBase, ReferenceGenome}

final case class PCanonicalLocus(rg: RGBase) extends PLocus with PComplex {

  val repr: PStruct = PCanonicalStruct(FastIndexedSeq((true, contigType), (true, positionType)),
    TStruct("locus" -> TString(), "position" -> TInt32()))

  def contigType: PString = PCanonicalString

  def positionType: PInt32 = PCanonicalInt32

  def loadContig(region: Region, off: Long): String = contigType.loadString(region, repr.loadField(region, off, 0))

  def loadPosition(region: Region, off: Long): Int = positionType.load(region, repr.loadField(region, off, 1))

  def loadContig(region: Code[Region], off: Code[Long]): Code[Long] = repr.loadField(region, off, 0)

  def loadPosition(region: Code[Region], off: Code[Long]): Code[Long] = repr.loadField(region, off, 1)

  def virtualType: Type = TLocus(rg)
}
