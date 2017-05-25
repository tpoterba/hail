package is.hail.annotations

import is.hail.expr._
import is.hail.utils._
import org.apache.spark.sql.Row
import org.apache.spark.unsafe.Platform

/**
  * Thoughts about design
  * Array should store n elems, total size?
  * Struct should store missing bits, offset of each elem
  * Set = sorted Array
  * Dict = sorted array of keys, array of values
  *
  *
  * MISSING BITS UP FRONT
  *  > number of elements of struct by 4 bytes
  *  >
  *
  *
  *
  *
  * SIZE OF ROW:
  * ===========
  * ceil(ELEMS / 32) + cumulative size of elems + arrays
  */

object UnsafeAnnotations {
  val missingBitIndices: Array[Int] = (0 until 32).map(i => 0x1 << i).toArray

  def isAligned(l: Long): Boolean = (l & 0x7) == 0
}

object UnsafeRowBuilder {
  val supported: Set[Type] = Set(TInt, TLong, TDouble, TFloat, TBoolean)
}

class UnsafeRowBuilder(t: TStruct) {
  private var memloc = Platform.allocateMemory(t.size)
  assert(isAligned)
  println(s"memLoc is $memloc")

  private var cursor = memloc

  private var i = 0

  private var offsets = new Array[Long](t.size)
  val missingBits = new Array[Int]((t.size + 31) / 32)

  cursor += 4 * missingBits.length

  private def isAligned: Boolean = (memloc & 0x7) == 0

  private def pad4() {
    val mod = cursor & 0x3
    if (mod != 0)
      cursor += 4 - mod
  }

  private def pad8() {
    val mod = cursor & 0x7
    if (mod != 0)
      cursor += 8 - mod
  }

  def put(a: Annotation) {
    if (a == null) {
      val bitIndex = i / 32
      val bitShift = i % 32

      missingBits(bitIndex) = missingBits(bitIndex) | (0x1 << bitShift)
    } else {
      t.fields(i).typ match {
        case TBoolean =>
          Platform.putBoolean(null, cursor, a.asInstanceOf[Boolean])
          cursor += 1
        case TInt =>
          pad4()
          Platform.putInt(null, cursor, a.asInstanceOf[Int])
          cursor += 4
        case TLong =>
          pad8()
          Platform.putLong(null, cursor, a.asInstanceOf[Long])
          cursor += 8
        case TFloat =>
          pad4()
          Platform.putFloat(null, cursor, a.asInstanceOf[Float])
          cursor += 4
        case TDouble =>
          pad8()
          Platform.putDouble(null, cursor, a.asInstanceOf[Double])
          cursor += 8
        case TString =>
      }
    }
  }

  def putInt(x: Int) {
    offsets(i) = cursor - memloc
    Platform.putInt(null, memloc + cursor, x)
    cursor += 4
    i += 1
  }

  def result(): UnsafeRow = {
    if (i < t.size)
      fatal(s"only wrote $i of ${ t.size } fields in UnsafeRowBuilder")
    new UnsafeRow(memloc, offsets, t)
  }
}

class UnsafeRow(m: Long, offsets: Array[Long], t: TStruct) extends Row {

  override def length: Int = ???

  override def get(i: Int): Any = ???

  override def getInt(i: Int): Int = {
    if (!isDefined(i))
      throw new NullPointerException
    Platform.getInt(null, offsets(i) + m)
  }

  override def copy(): Row = ???

  def free() {
    Platform.freeMemory(m)
  }

  private def isDefined(i: Int): Boolean = {
    val bitIndex = i / 32
    val bitShift = i % 32
    (Platform.getInt(null, m + bitIndex) & (0x1 << bitShift)) != 0
  }
}