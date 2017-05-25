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
  **/

object UnsafeAnnotations {
  val missingBitIndices: Array[Int] = (0 until 32).map(i => 0x1 << i).toArray

  def isAligned(l: Long): Boolean = (l & 0x7) == 0

  def memOffset(offset: Long): Long = Platform.LONG_ARRAY_OFFSET + offset
}

class UnsafeRowBuilder(t: TStruct, m: Array[Long] = null) {
  private var mem: Array[Long] = _
  if (m != null)
    mem = m
  else
    mem = new Array[Long](128)
  //  private var memloc = Platform.allocateMemory(t.size)
  //  assert(isAligned)
  println(s"memLoc is $mem")
  setMissingBitsZero()


  def setMissingBitsZero() {
    var i = 0
    while (i < ((t.size >> 5) << 2)) {
      Platform.putInt(mem, UnsafeAnnotations.memOffset(4 * i), 0)
      i += 1
    }
  }

  //  private var cursor = UnsafeAnnotations.LONG_ARRAY_OFFSET

  //  private var i = 0

  //  val missingBits = new Array[Int]((t.size + 31) / 32)

  //  cursor += 4 * missingBits.length

  //  private def isAligned: Boolean = (cursor & 0x7) == 0
  //
  //  private def pad4() {
  //    val mod = cursor & 0x3
  //    if (mod != 0)
  //      cursor += 4 - mod
  //  }
  //
  //  private def pad8() {
  //    val mod = cursor & 0x7
  //    if (mod != 0)
  //      cursor += 8 - mod
  //  }

  def putRow(r: Row) {
    assert(t.typeCheck(r))
    assert(r != null)

    var i = 0
    while (i < t.size) {
      if (r.isNullAt(i)) {
        //        println(s"inserting a null at position $i")
        val intIndex = UnsafeAnnotations.memOffset((i >> 5) << 2)
        val bitShift = i % 32
        val oldBits = Platform.getInt(mem, intIndex)
        val newBits = oldBits | (0x1 << bitShift)
        //        if (i == 33) {
        //          println(s"intIndex = $intIndex")
        //          println(s"bitShift = $bitShift")
        //          println(s"oldBits = $oldBits")
        //          println(s"newBits = $newBits")
        //        }
        Platform.putInt(mem, intIndex, newBits)
      } else {
        val off = UnsafeAnnotations.memOffset(t.byteOffsets(i))
        //        println(s"inserting value ${r.get(i)} at position $i")
        t.fields(i).typ match {
          case TBoolean => Platform.putByte(mem, off, r.getBoolean(i).toByte)
          case TInt => Platform.putInt(mem, off, r.getInt(i))
          case TLong => Platform.putLong(mem, off, r.getLong(i))
          case TFloat => Platform.putFloat(mem, off, r.getFloat(i))
          case TDouble => Platform.putDouble(mem, off, r.getDouble(i))
          case err =>
            println(s"Not supported: $err")
            ???
        }
      }
      i += 1
    }
  }


  def result(): UnsafeRow = {
    //    if (i < t.size)
    //      fatal(s"only wrote $i of ${ t.size } fields in UnsafeRowBuilder")
    new UnsafeRow(mem, t)
  }
}

class UnsafeRow(mem: Array[Long], t: TStruct) extends Row {

  override def length: Int = t.size

  override def get(i: Int): Any = {
    val off = UnsafeAnnotations.memOffset(t.byteOffsets(i))
    if (isNullAt(i))
      null
    else t.fields(i).typ match {
      case TBoolean =>
        val b = Platform.getByte(mem, off)
        if (b != 0 && b != 1)
          fatal(s"invalid bool byte: $b")
        b == 1
      case TInt => Platform.getInt(mem, off)
      case TLong => Platform.getLong(mem, off)
      case TFloat => Platform.getFloat(mem, off)
      case TDouble => Platform.getDouble(mem, off)
      case _ => ???
    }
  }

  //  override def getInt(i: Int): Int = {
  //    if (!isDefined(i))
  //      throw new NullPointerException
  //    Platform.getInt(mem, offsets(i) + UnsafeAnnotations.LONG_ARRAY_OFFSET)
  //  }

  override def copy(): Row = ???

  //  def free() {
  //    Platform.freeMemory(m)
  //  }

  override def isNullAt(i: Int): Boolean = {
    val intIndex = UnsafeAnnotations.memOffset((i >> 5) << 2)
    val bitShift = i % 32
    (Platform.getInt(mem, intIndex) & (0x1 << bitShift)) != 0
  }
}