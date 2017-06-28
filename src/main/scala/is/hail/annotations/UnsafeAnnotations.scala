package is.hail.annotations

import is.hail.expr._
import is.hail.utils._
import is.hail.variant.{AltAllele, Variant}
import org.apache.spark.sql.Row
import org.apache.spark.unsafe.Platform

import scala.collection.mutable.ArrayBuffer

object UnsafeAnnotations {
  def arrayElementSize(t: Type): Int = {
    var eltSize = t.byteSize
    val mod = eltSize % t.alignment
    if (mod != 0)
      eltSize += (t.alignment - mod)
    eltSize
  }

  def roundUpAlignment(offset: Int, alignment: Int): Int = {
    alignment match {
      case 1 => offset
      case 4 =>
        val mod = offset & 0x3
        if (mod != 0)
          offset + (4 - mod)
        else
          offset
      case 8 =>
        val mod = offset & 0x7
        if (mod != 0)
          offset + (8 - mod)
        else
          offset
      case _ => throw new AssertionError(s"unexpected alignment: $alignment")
    }
  }
}

class KVStructEmulator(var k: Any, var v: Any) extends Row {
  def length: Int = 2

  override def get(i: Int): Any = if (i == 0) k else if (i == 1) v else throw new IndexOutOfBoundsException

  override def copy(): Row = new KVStructEmulator(k, v)
}

class KVArrayEmulator(keys: ArrayBuffer[Any], values: ArrayBuffer[Any]) extends IndexedSeq[KVStructEmulator] {
  private val em = new KVStructEmulator(null, null)

  override def length: Int = {
    assert(keys.length == values.length)
    keys.length
  }

  override def apply(idx: Int): KVStructEmulator = {
    em.k = keys(idx)
    em.v = values(idx)
    em
  }
}

class UnsafeRowBuilder(t: TStruct, sizeHint: Int = 128, debug: Boolean = false) {
  private var buffer: MemoryBuffer = MemoryBuffer(sizeHint)

  private def putBinary(value: Array[Byte], offset: Int) {
    assert(offset % 4 == 0, s"invalid binary offset: $offset")

    buffer.align(4)

    buffer.storeInt(offset, buffer.offset)

    val totalSize = 4 + value.length

    buffer.appendInt(totalSize)
    buffer.appendBytes(value)
  }

  private def putArray(value: Iterable[_], offset: Int, elementType: Type) {
    assert(offset % 4 == 0, s"invalid array offset: $offset")

    val eltSize = UnsafeAnnotations.arrayElementSize(elementType)

    buffer.align(4)

    buffer.storeInt(offset, buffer.offset)

    val nElements = value.size
    buffer.appendInt(nElements)

    val missingBytesStart = buffer.allocate(value.size / 8 + 1)

    buffer.align(elementType.alignment)

    val elementStart = buffer.allocate(nElements * eltSize)
//    if (debug)
//      println(s"putting array ${ value.toSeq } at offset $offset with shift ${ appendPointer - offset }, totSize=$totalSize")

    // TODO: specialize to numeric types. Non-nullable types make this easier, too
    var i = 0
    value.foreach { elt =>
      if (elt == null) {
        val byteIndex = missingBytesStart + (i >> 3)
        val shift = i & 0x7
        val oldByte = buffer.loadByte(byteIndex)
        buffer.storeByte(byteIndex, (oldByte | (0x1 << (i & 0x7))).toByte)
      } else {
        put(elt, i * eltSize + elementStart, elementType)
      }
      i += 1
    }
  }

  private def putStruct(value: Row, offset: Int, struct: TStruct) {
    if (debug) println(s"inserting struct ${ value } / ${ struct.toPrettyString(compact = true) } at $offset")

    var i = 0
    while (i < struct.size) {
      if (value.isNullAt(i)) {
        val byteIndex = offset + (i >> 3)
        val shift = i & 0x7
        val oldByte = buffer.loadByte(byteIndex)
        buffer.storeByte(byteIndex, (oldByte | (0x1 << (i & 0x7))).toByte)
      } else {
        put(value.get(i), struct.byteOffsets(i) + offset, struct.fields(i).typ)
      }
      i += 1
    }
  }

  private def put(value: Annotation, offset: Int, elementType: Type) {
    assert(value != null, s"got a null value of type ${ elementType } at offset $offset")
    elementType match {
      case TInt | TCall => buffer.storeInt(offset, value.asInstanceOf[Int])
      case TLong => buffer.storeLong(offset, value.asInstanceOf[Long])
      case TFloat => buffer.storeFloat(offset, value.asInstanceOf[Float])
      case TDouble => buffer.storeDouble(offset, value.asInstanceOf[Double])
      case TBoolean => buffer.storeByte(offset, value.asInstanceOf[Boolean].toByte)
      case TString => putBinary(value.asInstanceOf[String].getBytes(), offset)
      case t: TContainer => putArray(value.asInstanceOf[Iterable[_]], offset, t.elementType)
      case t: TDict =>
        val m = value.asInstanceOf[Map[Any, Any]]
        val arr = m.keys.view.map(k => Row(k, m(k)))
        putArray(arr, offset, t.memStruct)
      case struct: TStruct =>
        if (struct.size > 0)
          putStruct(value.asInstanceOf[Row], offset, struct)

      case TAltAllele | TVariant | TGenotype | TLocus | TInterval =>
        val expandedType = Annotation.expandType(elementType).asInstanceOf[TStruct]
        if (debug)
          println(s"putting expanded struct $expandedType at $offset")
        val expandedAnnotation = Annotation.expandAnnotation(value, elementType).asInstanceOf[Row]
        putStruct(expandedAnnotation, offset, expandedType)

      case err => throw new NotImplementedError(err.toPrettyString(compact = true))
    }
  }

  def convert(r: Row): UnsafeRow = {
    require(r != null, "cannot convert null row")
    buffer.clear()
    val start = buffer.allocate(t.byteSize)
    assert(start == 0)
    putStruct(r, start, t)
    new UnsafeRow(new Pointer(buffer.result(), 0), t)
  }

//  private def clear() {
//    buffer.clear()
//  }
//
//
//  private def result(): UnsafeRow = {
//    val memCopy = new Array[Byte](appendPointer)
//    Platform.copyMemory(mem, Platform.BYTE_ARRAY_OFFSET, memCopy, Platform.BYTE_ARRAY_OFFSET, appendPointer)
//    new UnsafeRow(memCopy, t, debug = debug)
//  }
}

class UnsafeRow(ptr: Pointer, t: TStruct, debug: Boolean = false) extends Row {

  override def length: Int = t.size

  def readBinary(offset: Int): Array[Byte] = {
    val shift = ptr.loadInt(offset)
    assert(shift > 0 && (shift & 0x3) == 0, s"invalid shift: $shift, from offset $offset (shift offset $ptr)")

    val binStart = offset + shift
    val binLength = ptr.mem.loadInt(binStart)
//    if (debug)
//      println(s"reading type Binary of length $binLength from offset $offset (+$shiftOffset=${ offset + shiftOffset }) with shift $shift")
    ptr.loadBytes(binStart, binLength)
  }

  private def readArray(offset: Int, elementType: Type): IndexedSeq[Any] = {
    val shift = ptr.loadInt(offset)
//    assert(shift > 0 && (shift & 0x3) == 0, s"invalid shift: $shift, from offset $offset (shift offset $shiftOffset)")
//    println(s"trying to read array with type ${elementType.toPrettyString(compact = true)} from offset $offset+$shift")

    val arrStart = offset + shift
    val arrLength = ptr.mem.loadInt(arrStart)
    val missingBytes = (arrLength + 7) / 8
    val elemsStart = UnsafeAnnotations.roundUpAlignment(arrStart + 4 + missingBytes, elementType.byteSize)
    val eltSize = UnsafeAnnotations.arrayElementSize(elementType)
    if (debug)
      println(s"reading Array[${ elementType.toPrettyString(compact = true) }] of length $arrLength from $offset(+${ shiftOffset }+$shift=${ offset + shiftOffset + shift })")
    //    println(s"reading array at offset $offset with shift ${ shift }, has length $arrLength")

    //    println(s"t is $elementType")
    //    println(s"arrLength is $arrLength")

    val a = new Array[Any](arrLength)

    var i = 0
    while (i < arrLength) {

      val byteIndex = i >> 3
      val bitShift = i & 0x1f
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
      println(s"generating new UnsafeRow for type ${ struct.toPrettyString(compact = true) } at offset $offset(+$shiftOffset=${ offset + shiftOffset })")
    new UnsafeRow(mem, struct, offset + shiftOffset, debug)
  }

  private def read(offset: Int, t: Type): Any = {
    if (debug)
      println(s"reading type ${ t.toPrettyString(compact = true) } at offset $offset(+$shiftOffset=${ offset + shiftOffset })")
    t match {
      case TBoolean =>
        val b = readByte(offset)
        assert(b == 0 || b == 1, s"invalid boolean byte $b from offset $offset")
        b == 1
      case TInt | TCall => readInt(offset)
      case TLong => readLong(offset)
      case TFloat => readFloat(offset)
      case TDouble => readDouble(offset)
      case TArray(elt) => readArray(offset, elt)
      case TSet(elt) => readArray(offset, elt).toSet
      case TString => new String(readBinary(offset))
      case t: TDict =>
        println(s"trying to read dict with type ${t.memStruct.toPrettyString(compact = true)} from offset $offset+$shiftOffset")
        readArray(offset, t.memStruct).asInstanceOf[IndexedSeq[Row]].map(r => (r.get(0), r.get(1))).toMap
      case struct: TStruct =>
        if (struct.size == 0)
          Annotation.emptyRow
        else
          readStruct(offset, struct)

      case TVariant | TLocus | TAltAllele | TGenotype | TInterval =>
        val r = readStruct(offset, Annotation.expandType(t).asInstanceOf[TStruct])
        SparkAnnotationImpex.importAnnotation(r, t)

      case _ => ???
    }
  }

  private def assertDefined(i: Int) {
    if (isNullAt(i))
      throw new NullPointerException(s"null value at index $i")
  }

  override def get(i: Int): Any = {
    val offset = t.byteOffsets(i)
    if (debug)
      println(s"The offset for element $i (type ${ t.fields(i).typ }) is $offset(+${ shiftOffset }=${ offset + shiftOffset })")
    if (isNullAt(i))
      null
    else
      read(offset, t.fields(i).typ)
  }

  override def copy(): Row = new UnsafeRow(mem.clone(), t, shiftOffset, debug)

  override def getInt(i: Int): Int = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    readInt(offset)
  }

  override def getLong(i: Int): Long = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    readLong(offset)
  }

  override def getFloat(i: Int): Float = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    readFloat(offset)
  }

  override def getDouble(i: Int): Double = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    readDouble(offset)
  }

  override def getBoolean(i: Int): Boolean = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    readByte(offset) == 1
  }

  override def getByte(i: Int): Byte = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    readByte(offset)
  }

  def getBinary(i: Int): Array[Byte] = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    readBinary(offset)
  }

  override def isNullAt(i: Int): Boolean = {
    val intIndex = (i >> 5) << 2
    val bitShift = i & 0x1f
    (readInt(intIndex) & (0x1 << bitShift)) != 0
  }
}