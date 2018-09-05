package is.hail.expr.types.physical

import is.hail.annotations.Region
import is.hail.asm4s.Code

case object PCanonicalFloat32 extends PPrimitive(4) with PFloat32 {
  override def load(r: Region, addr: Long): Float = r.loadFloat(addr)

  override def load(r: Code[Region], addr: Code[Long]): Code[Float] = r.loadFloat(addr)
}
