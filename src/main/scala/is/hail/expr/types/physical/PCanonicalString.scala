package is.hail.expr.types.physical

import is.hail.annotations.Region
import is.hail.asm4s.{Code, const}
import is.hail.expr.types.{TString, Type}

case object PCanonicalString extends PPointer with PString {
  def virtualType: Type = TString()

  def contentAlignment: Long = 4

  def contentByteSize(length: Int): Long = 4 + length

  def contentByteSize(length: Code[Int]): Code[Long] = (const(4) + length).toL

  def loadLength(region: Region, boff: Long): Int =
    region.loadInt(boff)

  def loadLength(region: Code[Region], boff: Code[Long]): Code[Int] =
    region.loadInt(boff)

  def bytesOffset(boff: Long): Long = boff + 4

  def bytesOffset(boff: Code[Long]): Code[Long] = boff + 4L

  def allocate(region: Region, length: Int): Long = {
    region.allocate(contentAlignment, contentByteSize(length))
  }

  def allocate(region: Code[Region], length: Code[Int]): Code[Long] = {
    region.allocate(const(contentAlignment), contentByteSize(length))
  }

  def loadString(region: Region, boff: Long): String = {
    val length = loadLength(region, boff)
    new String(region.loadBytes(bytesOffset(boff), length))
  }

  def loadString(region: Code[Region], boff: Code[Long]): Code[String] = {
    val length = loadLength(region, boff)
    Code.newInstance[String, Array[Byte]](
      region.loadBytes(bytesOffset(boff), length))
  }
}
