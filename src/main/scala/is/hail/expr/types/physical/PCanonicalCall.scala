package is.hail.expr.types.physical

import is.hail.annotations.Region
import is.hail.asm4s.Code
import is.hail.expr.types.{TCall, Type}

case object PCanonicalCall extends PCall with PComplex {
  def repr: PInt32 = PCanonicalInt32

  def load(r: Region, addr: Long): Int = repr.load(r, addr)

  def load(r: Code[Region], addr: Code[Long]): Code[Int]  = repr.load(r, addr)

  override def load(region: Code[Region]): Code[Long] => Code[_] = repr.load(region)
}
