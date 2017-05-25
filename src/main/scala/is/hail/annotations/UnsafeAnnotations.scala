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

  def memOffset(offset: Int): Int = Platform.LONG_ARRAY_OFFSET + offset

  def nLongs(bytes: Int) = (bytes + 7) / 8

  def missingBytes(elems: Int): Int = (elems + 31) / 32 * 4

  def roundUpAlignment(offset: Int): Int = {
    val mod = offset % 8
    if (mod != 0)
      offset + (8 - mod)
    else
      offset
  }
}

class UnsafeRowBuilder(t: TStruct, m: Array[Long] = null) {
  private var mem: Array[Long] = _
  private var appendPointer = UnsafeAnnotations.roundUpAlignment(t.byteSize)
  if (m != null) {
    mem = m
    reallocate(UnsafeAnnotations.nLongs(appendPointer))
  }
  else
    mem = new Array[Long](UnsafeAnnotations.nLongs(appendPointer))

  //  private var memloc = Platform.allocateMemory(t.size)
  //  assert(isAligned)
//  println(s"memLoc is $mem")

  setMissingBitsZero()


  def reallocate(target: Int) {
    val nLongs = (target + 7) / 8

    if (nLongs > mem.length) {
      val newMem = if (nLongs < 2 * mem.length)
        new Array[Long](mem.length * 2)
      else
        new Array[Long](nLongs)
//      println(s"reallocated from ${ mem.length } to ${ newMem.length }")
      System.arraycopy(mem, 0, newMem, 0, mem.length)
      mem = newMem
    }
  }

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


  private def readInt(offset: Int): Int = Platform.getInt(mem, UnsafeAnnotations.memOffset(offset))

  private def putInt(value: Int, offset: Int): Int = {
    assert(offset % 4 == 0)
//    println(s"offset $offset writing $value")

    Platform.putInt(mem, UnsafeAnnotations.memOffset(offset), value)
    offset + 4
  }

  private def putLong(value: Long, offset: Int): Int = {
    assert(offset % 8 == 0)
//    println(s"offset $offset writing $value")

    Platform.putLong(mem, UnsafeAnnotations.memOffset(offset), value)
    offset + 8
  }

  private def putFloat(value: Float, offset: Int): Int = {
    assert(offset % 4 == 0)
//    println(s"offset $offset writing $value")

    Platform.putFloat(mem, UnsafeAnnotations.memOffset(offset), value)
    offset + 4
  }

  private def putDouble(value: Double, offset: Int): Int = {
    assert(offset % 8 == 0)
//    println(s"offset $offset writing $value")
    Platform.putDouble(mem, UnsafeAnnotations.memOffset(offset), value)
    offset + 8
  }

  private def putByte(value: Byte, offset: Int): Int = {
//    println(s"offset $offset writing $value")
    Platform.putByte(mem, UnsafeAnnotations.memOffset(offset), value)
    offset + 1
  }

  private def putBinary(value: Array[Byte], offset: Int): Int = {
    var cursor = appendPointer

    // write the offset
    putInt(appendPointer - offset, offset)

    val totalSize = 4 + value.length
    //    println(s"putting array ${ value } at offset $offset with shift ${ appendPointer - offset }, totSize=$totalSize")

    appendPointer = UnsafeAnnotations.roundUpAlignment(totalSize + appendPointer)
    reallocate(appendPointer)
    putInt(value.length, cursor)

    cursor += 4

    Platform.copyMemory(value, Platform.BYTE_ARRAY_OFFSET, mem, UnsafeAnnotations.memOffset(cursor), value.length)

    offset + 4
  }

  private def putArray(value: Iterable[_], offset: Int, elementType: Type): Int = {

    val missingBytes = UnsafeAnnotations.missingBytes(value.size)
    val eltSize = elementType.byteSize

    var cursor = appendPointer

    // write the offset
    putInt(appendPointer - offset, offset)

    val totalSize = UnsafeAnnotations.roundUpAlignment(
      UnsafeAnnotations.roundUpAlignment(4 + missingBytes) + value.size * eltSize)
//    println(s"putting array ${ value } at offset $offset with shift ${ appendPointer - offset }, totSize=$totalSize")

    appendPointer = totalSize + appendPointer
    reallocate(appendPointer)
    putInt(value.size, cursor)
//    println(s"wrote length ${ value.length } at cursor $cursor")

    cursor += 4
    val missingBitStart = cursor

    cursor += missingBytes

    // align the element
    cursor = UnsafeAnnotations.roundUpAlignment(cursor)
    val alignment = cursor % eltSize

    var i = 0
    val iter = value.iterator
    while (i < value.size) {
      val elt = iter.next()
      if (elt == null) {
        val intIndex = missingBitStart + ((i >> 5) << 2)
        val bitShift = i % 32
        val oldBits = readInt(intIndex)
        val newBits = oldBits | (0x1 << bitShift)
        putInt(newBits, intIndex)

        cursor += eltSize
      } else {
        val shift = put(elt, cursor, elementType)
        assert(shift == cursor + eltSize)
        cursor = shift
      }
      i += 1
    }

    offset + 4
  }

  private def put(value: Annotation, offset: Int, elementType: Type): Int = {
    elementType match {
      case TInt => putInt(value.asInstanceOf[Int], offset)
      case TLong => putLong(value.asInstanceOf[Long], offset)
      case TFloat => putFloat(value.asInstanceOf[Float], offset)
      case TDouble => putDouble(value.asInstanceOf[Double], offset)
      case TBoolean => putByte(value.asInstanceOf[Boolean].toByte, offset)
      case TArray(et) => putArray(value.asInstanceOf[IndexedSeq[_]], offset, et)
      case TSet(et) => putArray(value.asInstanceOf[Set[_]], offset, et)
      case TString => putBinary(value.asInstanceOf[String].getBytes(), offset)


//      case s: TStruct =>

      case _ => ???
    }
  }

  def ingest(r: Row) {
    assert(t.typeCheck(r))
    assert(r != null)

    var i = 0
    while (i < t.size) {
      if (r.isNullAt(i)) {
        //        println(s"inserting a null at position $i")
        val intIndex = (i >> 5) << 2
        val bitShift = i % 32
        val oldBits = readInt(intIndex)
        val newBits = oldBits | (0x1 << bitShift)
//        println(s"updating missing for $i($bitShift): $oldBits => $newBits")
        //        if (i == 33) {
        //          println(s"intIndex = $intIndex")
        //          println(s"bitShift = $bitShift")
        //          println(s"oldBits = $oldBits")
        //          println(s"newBits = $newBits")
        //        }
        putInt(newBits, intIndex)
        assert(readInt(intIndex) == newBits)
      } else {
        val off = t.byteOffsets(i)
        //        println(s"inserting value ${r.get(i)} at position $i")
        put(r.get(i), off, t.fields(i).typ)
//        t.fields(i).typ match {
//          case TBoolean => putByte(r.getBoolean(i).toByte, off)
//          case TInt => putInt(r.getInt(i), off)
//          case TLong => putLong(r.getLong(i), off)
//          case TFloat => putFloat(r.getFloat(i), off)
//          case TDouble => putDouble(r.getDouble(i), off)
//          case TArray(t) => putArray(r.getAs[IndexedSeq[_]](i), off, t)
//          case err =>
//            println(s"Not supported: $err")
//            ???
//        }
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


  private def readInt(offset: Int) = Platform.getInt(mem, UnsafeAnnotations.memOffset(offset))

  private def readLong(offset: Int) = Platform.getLong(mem, UnsafeAnnotations.memOffset(offset))

  private def readFloat(offset: Int) = Platform.getFloat(mem, UnsafeAnnotations.memOffset(offset))

  private def readDouble(offset: Int) = Platform.getDouble(mem, UnsafeAnnotations.memOffset(offset))

  private def readByte(offset: Int) = Platform.getByte(mem, UnsafeAnnotations.memOffset(offset))

  def readBinary(offset: Int): Array[Byte] = {
    val shift = readInt(offset)
    assert(shift > 0, s"invalid shift: $shift")

    val binStart = offset + shift
    val binLength = readInt(binStart)
    val arr = new Array[Byte](binLength)
    Platform.copyMemory(mem, UnsafeAnnotations.memOffset(binStart + 4), arr, Platform.BYTE_ARRAY_OFFSET, binLength)

    arr
  }

  private def readArray(offset: Int, elementType: Type): IndexedSeq[Any] = {
    val shift = readInt(offset)
    assert(shift > 0, s"invalid shift: $shift")

    val arrStart = offset + shift
    val arrLength = readInt(arrStart)
    val missingBytes = UnsafeAnnotations.missingBytes(arrLength)
    val elemsStart = UnsafeAnnotations.roundUpAlignment(arrStart + 4 + missingBytes)
    val elemSize = elementType.byteSize
//    println(s"reading array at offset $offset with shift ${ shift }, has length $arrLength")

    //    println(s"t is $elementType")
    //    println(s"arrLength is $arrLength")

    val a = new Array[Any](arrLength)

    var i = 0
    while (i < arrLength) {

      val intIndex = (i >> 5) << 2
      val bitShift = i % 32
      val missingInt = readInt(arrStart + 4 + intIndex)
      val isMissing = (missingInt & (0x1 << bitShift)) != 0

      if (!isMissing)
        a(i) = read(elemsStart + i * elemSize, elementType)

      i += 1
    }

    a: IndexedSeq[Any]
  }

  private def read(offset: Int, t: Type): Any = {
    t match {
      case TBoolean =>
        val b = readByte(offset)
        assert(b == 0 || b == 1, s"invalid bool byte: $b from offset $offset")
        b == 1
      case TInt => readInt(offset)
      case TLong => readLong(offset)
      case TFloat => readFloat(offset)
      case TDouble => readDouble(offset)
      case TArray(elt) => readArray(offset, elt)
      case TSet(elt) => readArray(offset, elt).toSet
      case TString => new String(readBinary(offset))

      case _ => ???
    }
  }

  override def get(i: Int): Any = {
    val offset = t.byteOffsets(i)
    if (isNullAt(i))
      null
    else
      read(offset, t.fields(i).typ)
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
    val intIndex = (i >> 5) << 2
    val bitShift = i % 32
    (readInt(intIndex) & (0x1 << bitShift)) != 0
  }
}