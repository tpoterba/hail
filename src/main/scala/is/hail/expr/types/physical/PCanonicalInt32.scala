package is.hail.expr.types.physical

import is.hail.annotations.Region
import is.hail.asm4s.Code

case object PCanonicalInt32 extends PPrimitive(4) with PInt32 {
  override def load(r: Region, addr: Long): Int = r.loadInt(addr)

  override def load(r: Code[Region], addr: Code[Long]): Code[Int] = r.loadInt(addr)
}
