package is.hail.annotations

import is.hail.expr._
import is.hail.utils._
import is.hail.variant.{AltAllele, Variant}
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

  def arrayElementSize(t: Type): Int = {
    var eltSize = t.byteSize
    val mod = eltSize % t.alignment
    if (mod != 0)
      eltSize += (t.alignment - mod)
    eltSize
  }

  def roundUpAlignment(offset: Int): Int = {
    val mod = offset % 8
    if (mod != 0)
      offset + (8 - mod)
    else
      offset
  }
}

class UnsafeRowBuilder(t: TStruct, m: Array[Long] = null, debug: Boolean = false) {
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
      if (debug)
        println(s"reallocated from ${ mem.length } to ${ newMem.length }")
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

  private def putInt(value: Int, offset: Int) {
    assert(offset % 4 == 0, s"invalid offset: $offset")
    if (debug)
      println(s"offset $offset writing $value")

    Platform.putInt(mem, UnsafeAnnotations.memOffset(offset), value)
  }

  private def putLong(value: Long, offset: Int) {
    assert(offset % 8 == 0)
    if (debug) println(s"offset $offset writing $value")

    Platform.putLong(mem, UnsafeAnnotations.memOffset(offset), value)
  }

  private def putFloat(value: Float, offset: Int) {
    assert(offset % 4 == 0)
    if (debug) println(s"offset $offset writing $value")

    Platform.putFloat(mem, UnsafeAnnotations.memOffset(offset), value)
  }

  private def putDouble(value: Double, offset: Int) {
    assert(offset % 8 == 0)
    if (debug) println(s"offset $offset writing $value")
    Platform.putDouble(mem, UnsafeAnnotations.memOffset(offset), value)
  }

  private def putByte(value: Byte, offset: Int) {
    if (debug) println(s"offset $offset writing $value")
    Platform.putByte(mem, UnsafeAnnotations.memOffset(offset), value)
  }

  private def putBinary(value: Array[Byte], offset: Int) {
    var cursor = appendPointer

    // write the offset
    putInt(appendPointer - offset, offset)

    val totalSize = 4 + value.length
    if (debug) println(s"putting binary ${ value } at offset $offset with shift ${ appendPointer - offset }, totSize=$totalSize")

    appendPointer = UnsafeAnnotations.roundUpAlignment(totalSize + appendPointer)
    reallocate(appendPointer)
    putInt(value.length, cursor)

    cursor += 4

    Platform.copyMemory(value, Platform.BYTE_ARRAY_OFFSET, mem, UnsafeAnnotations.memOffset(cursor), value.length)
  }

  private def putArray(value: Iterable[_], offset: Int, elementType: Type) {

    val missingBytes = UnsafeAnnotations.missingBytes(value.size)
    val eltSize = UnsafeAnnotations.arrayElementSize(elementType)

    var cursor = appendPointer

    // write the offset
    putInt(appendPointer - offset, offset)

    val totalSize = UnsafeAnnotations.roundUpAlignment(
      UnsafeAnnotations.roundUpAlignment(4 + missingBytes) + value.size * eltSize)
    if (debug) println(s"putting array ${ value.toSeq } at offset $offset with shift ${ appendPointer - offset }, totSize=$totalSize")

    appendPointer = totalSize + appendPointer
    reallocate(appendPointer)
    putInt(value.size, cursor)
    //    println(s"wrote length ${ value.length } at cursor $cursor")

    cursor += 4
    val missingBitStart = cursor

    cursor += missingBytes

    // align the element
    cursor = UnsafeAnnotations.roundUpAlignment(cursor)

    var i = 0
    val iter = value.iterator
    while (i < value.size) {
      val elt = iter.next()
      if (debug)
        println(s"array index $i: writing value $elt (${elementType.toPrettyString(compact = true)}) to offset $cursor")
      if (elt == null) {
        val intIndex = missingBitStart + ((i >> 5) << 2)
        val bitShift = i % 32
        val oldBits = readInt(intIndex)
        val newBits = oldBits | (0x1 << bitShift)
        putInt(newBits, intIndex)

      } else {
        val shift = put(elt, cursor, elementType)
      }
      cursor += eltSize
      i += 1
    }
  }

  private def putStruct(value: Row, offset: Int, struct: TStruct) {
    var i = 0
    //    println(s"struct: ${struct.toPrettyString(compact = true)}")
    //    println(s"size: ${struct.byteSize}")
    if (debug) println(s"inserting struct ${ value } / ${ struct.toPrettyString(compact = true) } at $offset")
    while (i < struct.size) {
      if (value.isNullAt(i)) {
        //        println(s"inserting a null at position $i")
        val intIndex = ((i >> 5) << 2) + offset
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
        if (debug) println(s"element [$i] (type ${ struct.fields(i).typ.toPrettyString(compact = true) }) at offset ${ struct.byteOffsets(i) + offset } is null")
        putInt(newBits, intIndex)
        assert(readInt(intIndex) == newBits)
      } else {
        val off = struct.byteOffsets(i) + offset
        if (debug)
          println(s"element [$i] inserting value ${ value.get(i) } at offset $off")
        put(value.get(i), off, struct.fields(i).typ)
        //        println(s"inserting value ${r.get(i)} at position $i")
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

  private def put(value: Annotation, offset: Int, elementType: Type) {
    elementType match {
      case TInt | TCall => putInt(value.asInstanceOf[Int], offset)
      case TLong => putLong(value.asInstanceOf[Long], offset)
      case TFloat => putFloat(value.asInstanceOf[Float], offset)
      case TDouble => putDouble(value.asInstanceOf[Double], offset)
      case TBoolean => putByte(value.asInstanceOf[Boolean].toByte, offset)
      case TString => putBinary(value.asInstanceOf[String].getBytes(), offset)
      case TArray(et) => putArray(value.asInstanceOf[IndexedSeq[_]], offset, et)
      case TSet(et) => putArray(value.asInstanceOf[Set[_]], offset, et)
      case TDict(kt, vt) =>
        val values = value.asInstanceOf[Map[_, _]].toArray
        putArray(values.map(_._1), offset, kt)
        putArray(values.map(_._2), offset + 4, vt)
      //        putArray(value.asInstanceOf[Map[_, _]].map { case (k, v) => Row(k, v) }, offset, TStruct("k" -> kt, "v" -> vt))
      //      case TVariant =>
      //        val v = value.asInstanceOf[Variant]
      //        val schema =
      //      case s: TStruct =>
      case struct: TStruct =>
        if (struct.size > 0)
          putStruct(value.asInstanceOf[Row], offset, struct)

      case TAltAllele | TVariant | TGenotype | TLocus | TInterval =>
        val expandedType = Annotation.expandType(elementType).asInstanceOf[TStruct]
        //        println(s"expanded is ${expanded.toPrettyString(compact = true)}, ${expanded.byteSize}")
        //        println(s"value = $value")
        //        println(s"expanded = ${Annotation.expandAnnotation(value, elementType).asInstanceOf[Row]}")
        if (debug)
          println(s"putting expanded struct $expandedType at $offset")
        val expandedAnnotation = Annotation.expandAnnotation(value, elementType).asInstanceOf[Row]
        assert(expandedType.typeCheck(expandedAnnotation))
        putStruct(expandedAnnotation, offset, expandedType)

      case err => throw new NotImplementedError(s"$err")
    }
  }

  def ingest(r: Row) {
    assert(t.typeCheck(r))
    assert(r != null)
    putStruct(r, 0, t)
    //    var i = 0
    //    while (i < t.size) {
    //      if (r.isNullAt(i)) {
    //        //        println(s"inserting a null at position $i")
    //        val intIndex = (i >> 5) << 2
    //        val bitShift = i % 32
    //        val oldBits = readInt(intIndex)
    //        val newBits = oldBits | (0x1 << bitShift)
    //        //        println(s"updating missing for $i($bitShift): $oldBits => $newBits")
    //        //        if (i == 33) {
    //        //          println(s"intIndex = $intIndex")
    //        //          println(s"bitShift = $bitShift")
    //        //          println(s"oldBits = $oldBits")
    //        //          println(s"newBits = $newBits")
    //        //        }
    //        putInt(newBits, intIndex)
    //        assert(readInt(intIndex) == newBits)
    //      } else {
    //        val off = t.byteOffsets(i)
    //        //        println(s"inserting value ${r.get(i)} at position $i")
    //        put(r.get(i), off, t.fields(i).typ)
    //        //        t.fields(i).typ match {
    //        //          case TBoolean => putByte(r.getBoolean(i).toByte, off)
    //        //          case TInt => putInt(r.getInt(i), off)
    //        //          case TLong => putLong(r.getLong(i), off)
    //        //          case TFloat => putFloat(r.getFloat(i), off)
    //        //          case TDouble => putDouble(r.getDouble(i), off)
    //        //          case TArray(t) => putArray(r.getAs[IndexedSeq[_]](i), off, t)
    //        //          case err =>
    //        //            println(s"Not supported: $err")
    //        //            ???
    //        //        }
    //      }
    //      i += 1
    //    }
  }


  def result(): UnsafeRow = {
    //    if (i < t.size)
    //      fatal(s"only wrote $i of ${ t.size } fields in UnsafeRowBuilder")
    new UnsafeRow(mem, t, debug = debug)
  }
}

class UnsafeRow(mem: Array[Long], t: TStruct, shiftOffset: Int = 0, debug: Boolean = false) extends Row {

  override def length: Int = t.size

  private def absolute(offset: Int): Int = Platform.LONG_ARRAY_OFFSET + shiftOffset + offset

  private def readInt(offset: Int) = Platform.getInt(mem, absolute(offset))

  private def readLong(offset: Int) = Platform.getLong(mem, absolute(offset))

  private def readFloat(offset: Int) = Platform.getFloat(mem, absolute(offset))

  private def readDouble(offset: Int) = Platform.getDouble(mem, absolute(offset))

  private def readByte(offset: Int) = Platform.getByte(mem, absolute(offset))

  def readBinary(offset: Int): Array[Byte] = {
    val shift = readInt(offset)
    assert(shift > 0, s"invalid shift: $shift, from offset $offset (shift offset $shiftOffset)")

    val binStart = offset + shift
    val binLength = readInt(binStart)
    if (debug)
      println(s"reading type Binary of length $binLength from offset $offset (+$shiftOffset=${offset+shiftOffset}) with shift $shift")
    val arr = new Array[Byte](binLength)
    Platform.copyMemory(mem, absolute(binStart + 4), arr, Platform.BYTE_ARRAY_OFFSET, binLength)

    arr
  }

  private def readArray(offset: Int, elementType: Type): IndexedSeq[Any] = {
    val shift = readInt(offset)
    assert(shift > 0, s"invalid shift: $shift")

    val arrStart = offset + shift
    val arrLength = readInt(arrStart)
    val missingBytes = UnsafeAnnotations.missingBytes(arrLength)
    val elemsStart = UnsafeAnnotations.roundUpAlignment(arrStart + 4 + missingBytes + shiftOffset) - shiftOffset
    val eltSize = UnsafeAnnotations.arrayElementSize(elementType)
    if (debug)
      println(s"reading Array[${ elementType.toPrettyString(compact = true) }] of length $arrLength from $offset(+${ shiftOffset }+$shift=${offset+shiftOffset+shift})")
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
        a(i) = read(elemsStart + i * eltSize, elementType)

      i += 1
    }

    a
  }

  private def readStruct(offset: Int, struct: TStruct): UnsafeRow = {
    if (debug)
      println(s"generating new UnsafeRow for type ${ struct.toPrettyString(compact = true) } at offset $offset(+$shiftOffset=${offset+shiftOffset})")
    new UnsafeRow(mem, struct, offset + shiftOffset, debug)
  }

  private def read(offset: Int, t: Type): Any = {
    if (debug)
      println(s"reading type ${ t.toPrettyString(compact = true) } at offset $offset(+$shiftOffset=${offset+shiftOffset})")
    t match {
      case TBoolean =>
        val b = readByte(offset)
        assert(b == 0 || b == 1, s"invalid bool byte: $b from offset $offset")
        b == 1
      case TInt | TCall => readInt(offset)
      case TLong => readLong(offset)
      case TFloat => readFloat(offset)
      case TDouble => readDouble(offset)
      case TArray(elt) => readArray(offset, elt)
      case TSet(elt) => readArray(offset, elt).toSet
      case TString => new String(readBinary(offset))
      case TDict(kt, vt) => readArray(offset, kt).zip(readArray(offset + 4, vt)).toMap
      case struct: TStruct =>
        if (struct.size == 0)
          Row()
        else
          readStruct(offset, struct)

      case TVariant | TLocus | TAltAllele | TGenotype | TInterval =>
        val r = readStruct(offset, Annotation.expandType(t).asInstanceOf[TStruct])
        SparkAnnotationImpex.importAnnotation(r, t)

      case _ => ???
    }
  }

  override def get(i: Int): Any = {
    val offset = t.byteOffsets(i)
    if (debug)
      println(s"The offset for element $i (type ${t.fields(i).typ}) is $offset(+${shiftOffset}=${offset+shiftOffset})")
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

  override def getInt(i: Int): Int = {
    val offset = t.byteOffsets(i)
    if (isNullAt(i))
      throw new NullPointerException(s"null value at index $i")
    readInt(offset)
  }

  override def getLong(i: Int): Long = {
    val offset = t.byteOffsets(i)
    if (isNullAt(i))
      throw new NullPointerException(s"null value at index $i")
    readLong(offset)
  }

  override def getFloat(i: Int): Float = {
    val offset = t.byteOffsets(i)
    if (isNullAt(i))
      throw new NullPointerException(s"null value at index $i")
    readFloat(offset)
  }

  override def getDouble(i: Int): Double = {
    val offset = t.byteOffsets(i)
    if (isNullAt(i))
      throw new NullPointerException(s"null value at index $i")
    readDouble(offset)
  }

  override def getBoolean(i: Int): Boolean = {
    val offset = t.byteOffsets(i)
    if (isNullAt(i))
      throw new NullPointerException(s"null value at index $i")
    readByte(offset) == 1
  }

  override def getByte(i: Int): Byte = {
    val offset = t.byteOffsets(i)
    if (isNullAt(i))
      throw new NullPointerException(s"null value at index $i")
    readByte(offset)
  }

  def getBinary(i: Int): Array[Byte] = {
    val offset = t.byteOffsets(i)
    if (isNullAt(i))
      throw new NullPointerException(s"null value at index $i")
    readBinary(offset)
  }

  override def isNullAt(i: Int): Boolean = {
    val intIndex = (i >> 5) << 2
    val bitShift = i % 32
    (readInt(intIndex) & (0x1 << bitShift)) != 0
  }
}