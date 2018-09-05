package is.hail.expr.types.physical

import is.hail.annotations._
import is.hail.asm4s._
import is.hail.expr.Parser
import is.hail.expr.types._
import is.hail.utils._
import is.hail.variant.{RGBase, ReferenceGenome}

object PType {
  def parseCanonical(s: String): PType = canonical(Parser.parseType(s))

  def canonical(t: Type): PType = {
    t match {
      case _: TInt32 => PCanonicalInt32
      case _: TInt64 => PCanonicalInt64
      case _: TFloat64 => PCanonicalFloat64
      case _: TFloat32 => PCanonicalFloat32
      case _: TBoolean => PCanonicalBoolean
      case _: TBinary => PCanonicalBinary
      case _: TString => PCanonicalString
      case _: TCall => PCanonicalCall
      case tl: TLocus => PCanonicalLocus(tl.rg)
      case t: TInterval => PCanonicalInterval(canonical(t.pointType))
      case t: TContainer => PCanonicalArray(canonical(t.elementType), t, t.elementType.required)
      case ts: TBaseStruct => PCanonicalStruct(ts.types.map(t => (t.required, canonical(t))), ts)
    }
  }

  def assertCanonical(pt: PType): Unit = {
    pt match {
      case PCanonicalInt32 =>
      case PCanonicalInt64 =>
      case PCanonicalFloat64 =>
      case PCanonicalFloat32 =>
      case PCanonicalBoolean =>
      case PCanonicalBinary =>
      case PCanonicalString =>
      case PCanonicalCall =>
      case PCanonicalLocus(_) =>
      case PCanonicalInterval(pointType) => assertCanonical(pointType)
      case PCanonicalArray(elementType, _, _) => assertCanonical(elementType)
      case PCanonicalStruct(fields, _) => fields.foreach { case (_, t) => assertCanonical(t) }
    }
  }
}

abstract class PType {
  def virtualType: Type

  def byteSize: Long

  def alignment: Long

  def load(region: Code[Region]): Code[Long] => Code[_]
}

abstract class PStruct extends PType {

  def virtualType: TBaseStruct

  def allocate(region: Region): Long

  def isFieldMissing(region: Code[Region], offset: Code[Long], fieldIdx: Int): Code[Boolean]

  def isFieldDefined(rv: RegionValue, fieldIdx: Int): Boolean =
    isFieldDefined(rv.region, rv.offset, fieldIdx)

  def isFieldDefined(region: Region, offset: Long, fieldIdx: Int): Boolean

  def isFieldDefined(region: Code[Region], offset: Code[Long], fieldIdx: Int): Code[Boolean] =
    !isFieldMissing(region, offset, fieldIdx)

  def setFieldMissing(region: Region, offset: Long, fieldIdx: Int): Unit

  def setFieldMissing(region: Code[Region], offset: Code[Long], fieldIdx: Int): Code[Unit]

  def fieldOffset(offset: Long, fieldIdx: Int): Long

  def fieldOffset(offset: Code[Long], fieldIdx: Int): Code[Long]

  def loadField(rv: RegionValue, fieldIdx: Int): Long = loadField(rv.region, rv.offset, fieldIdx)

  def loadField(region: Region, offset: Long, fieldIdx: Int): Long

  def loadField(region: Code[Region], offset: Code[Long], fieldIdx: Int): Code[Long]

  def types: Array[PType]

  def size: Int = types.length
}

abstract class PConstructableStruct extends PStruct {
  def clearMissingBits(region: Region, off: Long): Unit

  def clearMissingBits(region: Code[Region], off: Code[Long]): Code[Unit]

  def byteOffsets: Array[Long]
}

abstract class PArray extends PType with PPointer {
  override def virtualType: TContainer

  def loadLength(region: Region, aoff: Long): Int

  def loadLength(region: Code[Region], aoff: Code[Long]): Code[Int]

  def isElementMissing(region: Region, aoff: Long, i: Int): Boolean =
    !isElementDefined(region, aoff, i)

  def isElementDefined(region: Region, aoff: Long, i: Int): Boolean

  def isElementMissing(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Boolean] =
    !isElementDefined(region, aoff, i)

  def isElementDefined(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Boolean]

  def loadElement(region: Region, aoff: Long, length: Int, i: Int): Long

  def loadElement(region: Code[Region], aoff: Code[Long], length: Code[Int], i: Code[Int]): Code[Long]

  def loadElement(region: Region, aoff: Long, i: Int): Long

  def loadElement(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Long]

  def elementOffset(aoff: Long, length: Int, i: Int): Long

  def elementOffsetInRegion(region: Region, aoff: Long, i: Int): Long

  def elementOffset(aoff: Code[Long], length: Code[Int], i: Code[Int]): Code[Long]

  def elementOffsetInRegion(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Long]

  def contentsByteSize(length: Int): Long

  def contentsByteSize(length: Code[Int]): Code[Long]

  def elementType: PType
}

abstract class PConstructableArray extends PArray {
  def contentsAlignment: Long

  def elementByteSize: Long

  def contentsByteSize(length: Int): Long

  def clearMissingBits(region: Region, aoff: Long, length: Int)

  def initialize(region: Region, aoff: Long, length: Int): Unit

  def initialize(region: Code[Region], aoff: Code[Long], length: Code[Int], a: Settable[Int]): Code[Unit]

  def allocate(region: Region, length: Int): Long

  def setElementMissing(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Unit]

  def setElementMissing(region: Region, aoff: Long, i: Int): Unit

  def elementsOffset(length: Int): Long

  def elementsOffset(length: Code[Int]): Code[Long]

  def elementOffset(aoff: Long, length: Int, i: Int): Long

  def elementOffsetInRegion(region: Region, aoff: Long, i: Int): Long

  def elementOffset(aoff: Code[Long], length: Code[Int], i: Code[Int]): Code[Long]

  def elementOffsetInRegion(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Long]

}

abstract class PPrimitive(val byteSize: Long) extends PType {
  def alignment: Long = byteSize
}

trait PInt32 extends PType {
  def virtualType: Type = TInt32()

  def load(r: Region, addr: Long): Int

  def load(r: Code[Region], addr: Code[Long]): Code[Int]

  def load(region: Code[Region]): Code[Long] => Code[_] = load(region, _)
}

trait PInt64 extends PType {
  def virtualType: Type = TInt64()

  def load(r: Region, addr: Long): Long

  def load(r: Code[Region], addr: Code[Long]): Code[Long]

  def load(region: Code[Region]): Code[Long] => Code[_] = load(region, _)

  def store(r: Region, value: Long): Unit

  def store(r: Code[Region]): Code[_] => Code[Unit]

  def store
}

trait PFloat32 extends PType {
  def virtualType: Type = TFloat32()

  def load(r: Region, addr: Long): Float

  def load(r: Code[Region], addr: Code[Long]): Code[Float]

  def load(region: Code[Region]): Code[Long] => Code[_] = load(region, _)
}

trait PFloat64 extends PType {
  def virtualType: Type = TFloat64()

  def load(r: Region, addr: Long): Double

  def load(r: Code[Region], addr: Code[Long]): Code[Double]

  def load(region: Code[Region]): Code[Long] => Code[_] = load(region, _)
}

trait PBoolean extends PType {
  def virtualType: Type = TBoolean()

  def load(r: Region, addr: Long): Boolean

  def load(r: Code[Region], addr: Code[Long]): Code[Boolean]

  def load(region: Code[Region]): Code[Long] => Code[_] = load(region, _)
}

abstract class PPointer extends PPrimitive(8) {
  def dereference(r: Region, offset: Long): Long = r.loadAddress(offset)

  def dereference(r: Code[Region], offset: Code[Long]): Code[Long] = r.loadAddress(offset)

  def load(region: Code[Region]): Code[Long] => Code[_] = dereference(region, _)
}

trait PBinary extends PType {
  def contentAlignment: Long

  def contentByteSize(length: Int): Long

  def contentByteSize(length: Code[Int]): Code[Long]

  def loadLength(region: Region, boff: Long): Int

  def loadLength(region: Code[Region], boff: Code[Long]): Code[Int]

  def bytesOffset(boff: Long): Long

  def bytesOffset(boff: Code[Long]): Code[Long]

  def allocate(region: Region, length: Int): Long

  def allocate(region: Code[Region], length: Code[Int]): Code[Long]
}

trait PString extends PBinary {
  def loadString(region: Region, boff: Long): String

  def loadString(region: Code[Region], boff: Code[Long]): Code[String]
}

case object PVoid extends PPrimitive(0) {
  def virtualType: Type = TVoid

  override def load(region: Code[Region]): Code[Long] => Code[_] = ???
}

trait PComplex extends PType {
  def repr: PType

  def byteSize: Long = repr.byteSize

  def alignment: Long = repr.alignment

  def load(region: Code[Region]): Code[Long] => Code[_] = repr.load(region)

  def store(region: Code[Region]): Code[_] => Code[Unit] = repr.store(region)
}

trait PInterval extends PType {
  def pointType: PType

  def virtualType: TInterval

  def startOffset(off: Code[Long]): Code[Long]

  def endOffset(off: Code[Long]): Code[Long]

  def loadStart(region: Region, off: Long): Long

  def loadStart(region: Code[Region], off: Code[Long]): Code[Long]

  def loadStart(rv: RegionValue): Long

  def loadEnd(region: Region, off: Long): Long

  def loadEnd(region: Code[Region], off: Code[Long]): Code[Long]

  def loadEnd(rv: RegionValue): Long

  def startDefined(region: Region, off: Long): Boolean

  def endDefined(region: Region, off: Long): Boolean

  def includesStart(region: Region, off: Long): Boolean

  def includesEnd(region: Region, off: Long): Boolean

  def startDefined(region: Code[Region], off: Code[Long]): Code[Boolean]

  def endDefined(region: Code[Region], off: Code[Long]): Code[Boolean]

  def includeStart(region: Code[Region], off: Code[Long]): Code[Boolean]

  def includeEnd(region: Code[Region], off: Code[Long]): Code[Boolean]
}

trait PCall {
  def virtualType: Type = TCall()

  def load(r: Region, addr: Long): Int

  def load(r: Code[Region], addr: Code[Long]): Code[Int]

  def load(region: Code[Region]): Code[Long] => Code[_] = load(region, _)
}

trait PLocus extends PType {
  def rg: RGBase

  def contigType: PString

  def positionType: PInt32

  def loadContig(region: Region, off: Long): String

  def loadPosition(region: Region, off: Long): Int

  def loadContig(region: Code[Region], off: Code[Long]): Code[Long]

  def loadPosition(region: Code[Region], off: Code[Long]): Code[Long]
}