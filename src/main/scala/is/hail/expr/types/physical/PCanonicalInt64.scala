package is.hail.expr.types.physical

import is.hail.annotations.Region
import is.hail.asm4s.Code

case object PCanonicalInt64 extends PPrimitive(8) with PInt64 {
  override def load(r: Region, addr: Long): Long = r.loadLong(addr)

  override def load(r: Code[Region], addr: Code[Long]): Code[Long] = r.loadLong(addr)
}
