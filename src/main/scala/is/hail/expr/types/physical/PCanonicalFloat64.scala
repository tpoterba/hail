package is.hail.expr.types.physical

import is.hail.annotations.Region
import is.hail.asm4s.Code

case object PCanonicalFloat64 extends PPrimitive(8) with PFloat64 {
  override def load(r: Region, addr: Long): Double = r.loadDouble(addr)

  override def load(r: Code[Region], addr: Code[Long]): Code[Double] = r.loadDouble(addr)
}
