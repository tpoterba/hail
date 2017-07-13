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
    if (eltSize > 0) {
      val mod = eltSize % t.alignment
      if (mod != 0)
        eltSize += (t.alignment - mod)
    }
    eltSize
  }

  def roundUpAlignment(offset: Int, alignment: Int): Int = {
    alignment match {
      case 0 => offset
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

class UnsafeRowBuilder(t: TStruct, sizeHint: Int = 128, debug: Boolean = false) {
  private var buffer: MemoryBuffer = new MemoryBuffer(sizeHint)

  private def putBinary(value: Array[Byte], offset: Int) {
    assert(offset % 4 == 0, s"invalid binary offset: $offset")

    buffer.align(4)

    buffer.storeInt(offset, buffer.offset)

    if (debug)
      println(s"putting array ${ value.toSeq } at offset $offset with a pointer to ${ buffer.offset }")

    buffer.appendInt(value.length)
    buffer.appendBytes(value)

    if (debug)
      println(s"after putting binary, offset is now ${ buffer.offset }, start=${buffer.offset - value.size}, bytes=${buffer.loadBytes(buffer.offset - value.size, value.size).toSeq}")
  }

  private def putArray(value: Iterable[_], offset: Int, elementType: Type) {
    assert(offset % 4 == 0, s"invalid array offset: $offset")

    val eltSize = UnsafeAnnotations.arrayElementSize(elementType)

    buffer.align(4)

    buffer.storeInt(offset, buffer.offset)
    if (debug)
      println(s"storing array at at ${ offset } -> ${ buffer.offset }")

    val nElements = value.size
    buffer.appendInt(nElements)

    val missingBytesStart = buffer.allocate((value.size + 7) / 8)

    buffer.align(elementType.alignment)

    val elementStart = buffer.allocate(nElements * eltSize)

    // TODO: specialize to numeric types. Non-nullable types make this easier, too
    var i = 0
    value.foreach { elt =>
      if (elt == null) {
        val byteIndex = missingBytesStart + (i / 8)
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
    if (debug) println(
      s"""inserting struct at $offset:
         |    ${ value }
         |    ${ struct.toPrettyString(compact = true) }
         |    ${ struct.byteOffsets.mkString(", ") }""".stripMargin)

    var i = 0
    while (i < struct.size) {
      if (value.isNullAt(i)) {
        val byteIndex = offset + (i / 8)
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
      case t: TArray => putArray(value.asInstanceOf[Iterable[_]], offset, t.elementType)
      case t: TSet => putArray(value.asInstanceOf[Iterable[_]], offset, t.elementType)
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
    if (debug) {
      println(s"size of $t is ${ t.byteSize }")
      println(s"byteIndices: \n  ${t.byteOffsets.zip(t.fields.map(_.typ)).mkString("\n  ")}")

    }
    buffer.clear()

    val start = buffer.allocate(t.byteSize)

    assert(start == 0)

    putStruct(r, start, t)
    new UnsafeRow(new Pointer(buffer.result(), 0), t, debug)
  }
}

class UnsafeRow(ptr: Pointer, t: TStruct, debug: Boolean = false) extends Row {

  override def length: Int = t.size

  private def readBinaryAbsolute(offset: Int): Array[Byte] = {
    val start = ptr.mem.loadInt(offset)

    readBytesAbsolute(start)
  }

  private def readBytesAbsolute(offset: Int): Array[Byte] = {
    assert(offset > 0 && (offset & 0x3) == 0, s"invalid binary start: $offset")
    val binLength = ptr.mem.loadInt(offset)
    val b = ptr.mem.loadBytes(offset + 4, binLength)
    if (debug)
      println(s"from absolute offset $offset, read length ${binLength}, bytes=${b.toSeq}")

    b
  }

  private def readBinary(offset: Int): Array[Byte] = {
    val start = ptr.loadInt(offset)
    if (debug)
      println(s"reading binary from initial offset $offset(+${ ptr.memOffset }=${ ptr.memOffset + offset }), going to $start")

    readBytesAbsolute(start)
  }

  private def readArrayElems(offset: Int, elementType: Type): IndexedSeq[Any] = {
    assert(offset > 0 && (offset & 0x3) == 0, s"invalid array start: $offset")

    val arrLength = ptr.mem.loadInt(offset)
    val missingBytes = (arrLength + 7) / 8
    val elemsStart = UnsafeAnnotations.roundUpAlignment(offset + 4 + missingBytes, elementType.alignment)
    val eltSize = UnsafeAnnotations.arrayElementSize(elementType)

    val a = new Array[Any](arrLength)

    if (debug)
      println(s"reading array from absolute offset $offset. Length=$arrLength, elemsStart=$elemsStart, elemSize=$eltSize")

    var i = 0
    while (i < arrLength) {

      val byteIndex = i / 8
      val bitShift = i & 0x7
      val missingByte = ptr.mem.loadByte(offset + 4 + byteIndex)
      val isMissing = (missingByte & (0x1 << bitShift)) != 0

      if (!isMissing)
        a(i) = readAbsolute(elemsStart + i * eltSize, elementType)

      i += 1
    }

    a
  }

  private def readArrayAbsolute(offset: Int, elementType: Type): IndexedSeq[Any] = {
    val start = ptr.mem.loadInt(offset)
    if (debug)
      println(s"reading array from $offset -> ${ start }")
    //    assert(shift > 0 && (shift & 0x3) == 0, s"invalid shift: $shift, from offset $offset (shift offset $shiftOffset)")
    //    println(s"trying to read array with type ${elementType.toPrettyString(compact = true)} from offset $offset+$shift")
    readArrayElems(start, elementType)
  }


  private def readArray(offset: Int, elementType: Type): IndexedSeq[Any] = {
    val start = ptr.loadInt(offset)
    if (debug)
      println(s"reading array from ${offset}+${ptr.memOffset}=${offset + ptr.memOffset} -> $start")
    readArrayElems(start, elementType)
  }

  private def readStructAbsolute(offset: Int, struct: TStruct): UnsafeRow = {
    if (debug)
      println(s"reading struct from offset ${ offset }+${ ptr.memOffset }=${ offset + ptr.memOffset }")
    new UnsafeRow(new Pointer(ptr.mem, offset), struct, debug)
  }

  private def readStruct(offset: Int, struct: TStruct): UnsafeRow = {
    if (debug)
      println(s"reading struct from offset ${ offset }")
    new UnsafeRow(new Pointer(ptr.mem, ptr.memOffset + offset), struct, debug)
  }

  private def readAbsolute(offset: Int, t: Type): Any = {
    t match {
      case TBoolean =>
        val b = ptr.mem.loadByte(offset)
        assert(b == 0 || b == 1, s"invalid boolean byte $b from offset $offset")
        b == 1
      case TInt | TCall => ptr.mem.loadInt(offset)
      case TLong => ptr.mem.loadLong(offset)
      case TFloat => ptr.mem.loadFloat(offset)
      case TDouble => ptr.mem.loadDouble(offset)
      case TArray(elt) => readArrayAbsolute(offset, elt)
      case TSet(elt) => readArrayAbsolute(offset, elt).toSet
      case TString => new String(readBinaryAbsolute(offset))
      case t: TDict =>
        readArrayAbsolute(offset, t.memStruct).asInstanceOf[IndexedSeq[Row]].map(r => (r.get(0), r.get(1))).toMap
      case struct: TStruct =>
        if (struct.size == 0)
          Annotation.empty
        else
          readStructAbsolute(offset, struct)

      case TVariant | TLocus | TAltAllele | TGenotype | TInterval =>
        val r = readStructAbsolute(offset, Annotation.expandType(t).asInstanceOf[TStruct])
        SparkAnnotationImpex.importAnnotation(r, t)

      case _ => ???
    }
  }

  private def read(offset: Int, t: Type): Any = {
    t match {
      case TBoolean =>
        val b = ptr.loadByte(offset)
        assert(b == 0 || b == 1, s"invalid boolean byte $b from offset $offset")
        b == 1
      case TInt | TCall => ptr.loadInt(offset)
      case TLong => ptr.loadLong(offset)
      case TFloat => ptr.loadFloat(offset)
      case TDouble => ptr.loadDouble(offset)
      case TArray(elt) => readArray(offset, elt)
      case TSet(elt) => readArray(offset, elt).toSet
      case TString => new String(readBinary(offset))
      case t: TDict =>
        //        println(s"trying to read dict with type ${t.memStruct.toPrettyString(compact = true)} from offset $offset+$shiftOffset")
        readArray(offset, t.memStruct).asInstanceOf[IndexedSeq[Row]].map(r => (r.get(0), r.get(1))).toMap
      case struct: TStruct =>
        if (struct.size == 0)
          Annotation.empty
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
    if (isNullAt(i))
      null
    else
      read(offset, t.fields(i).typ)
  }

  override def copy(): Row = new UnsafeRow(ptr.copy(), t, debug)

  override def getInt(i: Int): Int = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    ptr.loadInt(offset)
  }

  override def getLong(i: Int): Long = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    ptr.loadLong(offset)
  }

  override def getFloat(i: Int): Float = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    ptr.loadFloat(offset)
  }

  override def getDouble(i: Int): Double = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    ptr.loadDouble(offset)
  }

  override def getBoolean(i: Int): Boolean = {
    getByte(i) == 1
  }

  override def getByte(i: Int): Byte = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    ptr.loadByte(offset)
  }

  def getBinary(i: Int): Array[Byte] = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    readBinary(offset)
  }

  override def isNullAt(i: Int): Boolean = {
    if (i < 0 || i >= t.size)
      throw new IndexOutOfBoundsException(i.toString)
    val byteIndex = i / 8
    val bitShift = i & 0x7
    (ptr.loadByte(byteIndex) & (0x1 << bitShift)) != 0
  }
}