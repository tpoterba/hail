package is.hail.expr.types.physical

import is.hail.annotations.Region
import is.hail.asm4s.Code

case object PCanonicalBoolean extends PPrimitive(1) with PBoolean {
  override def load(r: Region, addr: Long): Boolean = r.loadBoolean(addr)

  override def load(r: Code[Region], addr: Code[Long]): Code[Boolean] = r.loadBoolean(addr)
}
