package is.hail.types.physical

import is.hail.annotations._
import is.hail.asm4s.{Code, _}
import is.hail.expr.ir.{EmitCodeBuilder, EmitMethodBuilder}
import is.hail.types.physical.stypes.{SCanonicalFloat32, SType}
import is.hail.types.virtual.TFloat32

case object PFloat32Optional extends PFloat32(false)
case object PFloat32Required extends PFloat32(true)

class PFloat32(override val required: Boolean) extends PNumeric with PPrimitive {
  lazy val virtualType: TFloat32.type = TFloat32

  override type NType = PFloat32

  def _asIdent = "float32"

  override def _pretty(sb: StringBuilder, indent: Int, compact: Boolean): Unit = sb.append("PFloat32")

  override def unsafeOrdering(): UnsafeOrdering = new UnsafeOrdering {
    def compare(o1: Long, o2: Long): Int = {
      java.lang.Float.compare(Region.loadFloat(o1), Region.loadFloat(o2))
    }
  }

  def codeOrdering(mb: EmitMethodBuilder[_], other: PType): CodeOrdering = {
    assert(other isOfType this)
    new CodeOrdering {
      type T = Float

      def compareNonnull(x: Code[T], y: Code[T]): Code[Int] =
        Code.invokeStatic2[java.lang.Float, Float, Float, Int]("compare", x, y)

      override def ltNonnull(x: Code[T], y: Code[T]): Code[Boolean] = x < y

      override def lteqNonnull(x: Code[T], y: Code[T]): Code[Boolean] = x <= y

      override def gtNonnull(x: Code[T], y: Code[T]): Code[Boolean] = x > y

      override def gteqNonnull(x: Code[T], y: Code[T]): Code[Boolean] = x >= y

      override def equivNonnull(x: Code[T], y: Code[T]): Code[Boolean] = x.ceq(y)
    }
  }

  override def byteSize: Long = 4

  override def zero = coerce[PFloat32](const(0.0f))

  override def add(a: Code[_], b: Code[_]): Code[PFloat32] = {
    coerce[PFloat32](coerce[Float](a) + coerce[Float](b))
  }

  override def multiply(a: Code[_], b: Code[_]): Code[PFloat32] = {
    coerce[PFloat32](coerce[Float](a) * coerce[Float](b))
  }

  override def sType: SType = SCanonicalFloat32

  def storePrimitiveAtAddress(cb: EmitCodeBuilder, addr: Code[Long], value: PCode): Unit =
    cb.append(Region.storeFloat(addr, value.asFloat.floatValue(cb)))
}

object PFloat32 {
  def apply(required: Boolean = false): PFloat32 = if (required) PFloat32Required else PFloat32Optional

  def unapply(t: PFloat32): Option[Boolean] = Option(t.required)
}
