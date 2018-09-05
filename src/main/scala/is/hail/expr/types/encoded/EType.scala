package is.hail.expr.types.encoded

import is.hail.expr.types._
import is.hail.expr.types.physical._

abstract class EType {
  // TODO: relax 1-to-1 mapping of encoded to physical types
  def physicalType: PType
}

case class EDefault(physicalType: PType) extends EType

abstract class EStruct extends EType {
  def physicalType: PStruct
}

abstract class EArray extends EType {
  def physicalType: PArray
}

case object EInt32 extends EType {
  def physicalType: PType = PInt32
}

abstract class EInt64 extends EType {
  def virtualType: Type = TInt64()

  def physicalType: PType = PInt64
}

abstract class EFloat32 extends EType {
  def virtualType: Type = TFloat32()

  def physicalType: PType = PFloat32
}

abstract class EFloat64 extends EType {
  def virtualType: Type = TFloat64()

  def physicalType: PType = PFloat64
}

abstract class EBool extends EType {
  def virtualType: Type = TBoolean()

  def physicalType: PType = PBool
}

abstract class EBinary extends EType {
  def virtualType: Type = TBinary()

  def physicalType: PType = PBool
}
