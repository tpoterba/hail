package is.hail.io

import java.io._

import is.hail.annotations._
import is.hail.asm4s._
import is.hail.expr.ir
import is.hail.expr.ir.{EmitFunctionBuilder, EmitUtils, EstimableEmitter, MethodBuilderSelfLike, PruneDeadFields}
import is.hail.expr.types.encoded._
import is.hail.expr.types.physical._
import is.hail.expr.types.virtual._
import is.hail.nativecode._
import is.hail.utils._

case class PackCodecSpec(child: BufferSpec) extends CodecSpec {
  def makeCodecSpec2(pType: PType) = PackCodecSpec2(pType, child)
}

final case class PackCodecSpec2(typ: PType, child: BufferSpec) extends CodecSpec2 {
  val encodedType: EType = EType.defaultFromPType(typ)
  def encodedVirtualType: Type = typ.virtualType

  def computeSubsetPType(requestedType: Type): PType = {
    assert(PruneDeadFields.isSupertype(requestedType, typ.virtualType))
    PType.canonical(requestedType)
  }

  def buildEncoder(t: PType): (OutputStream) => Encoder = {
    val f = EType.buildEncoder(encodedType, t)
    out: OutputStream => new CompiledEncoder(child.buildOutputBuffer(out), f)
  }

  def buildDecoder(requestedType: Type): (PType, (InputStream) => Decoder) = {
    val (rt, f) = EType.buildDecoder(encodedType, requestedType)
    (rt, (in: InputStream) => new CompiledDecoder(child.buildInputBuffer(in), f))
  }

  def buildCodeInputBuffer(is: Code[InputStream]): Code[InputBuffer] = child.buildCodeInputBuffer(is)

  def buildCodeOutputBuffer(os: Code[OutputStream]): Code[OutputBuffer] = child.buildCodeOutputBuffer(os)

  def buildEmitDecoderF[T](requestedType: Type, fb: EmitFunctionBuilder[_]): (PType, StagedDecoderF[T]) = {
    val rt = encodedType.decodedPType(requestedType)
    val mb = encodedType.buildDecoderMethod(rt, fb)
    (rt, (region: Code[Region], buf: Code[InputBuffer]) => mb.invoke[T](region, buf))
  }

  def buildEmitEncoderF[T](t: PType, fb: EmitFunctionBuilder[_]): StagedEncoderF[T] = {
    val mb = encodedType.buildEncoderMethod(t, fb)
    (region: Code[Region], off: Code[T], buf: Code[OutputBuffer]) => mb.invoke[Unit](off, buf)
  }
}

object ShowBuf {
  def apply(buf: Array[Byte], pos: Int, n: Int): Unit = {
    val sb = new StringBuilder()
    val len = if (n < 32) n else 32
    var j = 0
    while (j < len) {
      val x = (buf(pos + j).toInt & 0xff)
      if (x <= 0xf) sb.append(s" 0${ x.toHexString }") else sb.append(s" ${ x.toHexString }")
      if ((j & 0x7) == 0x7) sb.append("\n")
      j += 1
    }
    System.err.println(sb.toString())
  }

  def apply(addr: Long, n: Int): Unit = {
    val sb = new StringBuilder()
    val len = if (n < 32) n else 32
    var j = 0
    while (j < len) {
      val x = (Memory.loadByte(addr + j).toInt & 0xff)
      if (x <= 0xf) sb.append(s" 0${ x.toHexString }") else sb.append(s" ${ x.toHexString }")
      if ((j & 0x7) == 0x7) sb.append("\n")
      j += 1
    }
    System.err.println(sb.toString())
  }
}
