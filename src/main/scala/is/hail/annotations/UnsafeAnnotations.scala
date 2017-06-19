package is.hail.annotations

import is.hail.expr._
import is.hail.utils._
import is.hail.variant.{AltAllele, Variant}
import org.apache.spark.sql.Row
import org.apache.spark.unsafe.Platform

import scala.collection.mutable.ArrayBuffer

object UnsafeAnnotations {
  def nMissingBytes(elems: Int): Int = (elems + 31) / 32 * 4

  def arrayElementSize(t: Type): Int = {
    var eltSize = t.byteSize
    val mod = eltSize % t.alignment
    if (mod != 0)
      eltSize += (t.alignment - mod)
    eltSize
  }

  def roundUpAlignment(offset: Int): Int = {
    val mod = offset & 0x7
    if (mod != 0)
      offset + (8 - mod)
    else
      offset
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

class UnsafeRowBuilder(t: TStruct, sizeHint: Int = 0, debug: Boolean = false) {
  private var appendPointer: Int = _
  initializeAppendPointer()
  private var mem: Array[Byte] = new Array[Byte](math.max(sizeHint, appendPointer))

  private val dictKeyBuffer = new ArrayBuffer[Any]()
  private val dictValueBuffer = new ArrayBuffer[Any]()
  private val dictArrayEmulator = new KVArrayEmulator(dictKeyBuffer, dictValueBuffer)

  private def absolute(offset: Int): Int = Platform.BYTE_ARRAY_OFFSET + offset

  private def reallocate(target: Int) {
    if (target > mem.length) {
      val newMem = if (target < 2 * mem.length)
        new Array[Byte](mem.length * 2)
      else
        new Array[Byte](target)
      if (debug)
        println(s"reallocated from ${ mem.length } to ${ newMem.length }")
      System.arraycopy(mem, 0, newMem, 0, mem.length)
      mem = newMem
    }
  }

  private def initializeAppendPointer() {
    appendPointer = UnsafeAnnotations.roundUpAlignment(t.byteSize)
  }

  private def readInt(offset: Int): Int = Platform.getInt(mem, absolute(offset))

  private def putInt(value: Int, offset: Int) {
    assert((offset & 0x3) == 0, s"invalid int offset: $offset")
    if (debug) println(s"offset $offset writing int $value")

    Platform.putInt(mem, absolute(offset), value)
  }

  private def putLong(value: Long, offset: Int) {
    assert((offset & 0x7) == 0, s"invalid long offset: $offset")
    if (debug) println(s"offset $offset writing long $value")

    Platform.putLong(mem, absolute(offset), value)
  }

  private def putFloat(value: Float, offset: Int) {
    assert((offset & 0x3) == 0, s"invalid float offset: $offset")
    if (debug) println(s"offset $offset writing float $value")

    Platform.putFloat(mem, absolute(offset), value)
  }

  private def putDouble(value: Double, offset: Int) {
    assert((offset & 0x7) == 0, s"invalid double offset: $offset")
    if (debug) println(s"offset $offset writing double $value")
    Platform.putDouble(mem, absolute(offset), value)
  }

  private def putByte(value: Byte, offset: Int) {
    if (debug) println(s"offset $offset writing byte $value")
    Platform.putByte(mem, absolute(offset), value)
  }

  private def putBinary(value: Array[Byte], offset: Int) {
    var cursor = appendPointer

    // write the offset
    putInt(appendPointer - offset, offset)

    val totalSize = 4 + value.length
    if (debug)
      println(s"putting binary ${ value } at offset $offset with shift ${ appendPointer - offset }, totSize=$totalSize")

    appendPointer = UnsafeAnnotations.roundUpAlignment(totalSize + appendPointer)
    reallocate(appendPointer)

    putInt(value.length, cursor)

    cursor += 4

    Platform.copyMemory(value, Platform.BYTE_ARRAY_OFFSET, mem, absolute(cursor), value.length)
  }

  private def putArray(value: Iterable[_], offset: Int, elementType: Type) {

    val missingBytes = UnsafeAnnotations.nMissingBytes(value.size)
    val eltSize = UnsafeAnnotations.arrayElementSize(elementType)

    var cursor = appendPointer

    // write the offset
    putInt(appendPointer - offset, offset)

    val totalSize = UnsafeAnnotations.roundUpAlignment(
      UnsafeAnnotations.roundUpAlignment(4 + missingBytes) + value.size * eltSize)
    println(s"total size of ${elementType} at offset $offset is $totalSize")

    if (debug)
      println(s"putting array ${ value.toSeq } at offset $offset with shift ${ appendPointer - offset }, totSize=$totalSize")

    appendPointer = totalSize + appendPointer
    reallocate(appendPointer)
    putInt(value.size, cursor)

    cursor += 4
    val missingBitStart = cursor

    cursor += missingBytes

    // align the element
    cursor = UnsafeAnnotations.roundUpAlignment(cursor)

    // TODO: specialize to numeric types. Non-nullable types make this easier, too
    var i = 0
    val iter = value.iterator
    while (i < value.size) {
      val elt = iter.next()
      if (debug)
        println(s"array index $i: writing value $elt (${ elementType.toPrettyString(compact = true) }) to offset $cursor")
      if (elt == null) {
        val intIndex = missingBitStart + ((i >> 5) << 2)
        val bitShift = i & 0x1f
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
    if (debug) println(s"inserting struct ${ value } / ${ struct.toPrettyString(compact = true) } at $offset")

    var i = 0
    while (i < struct.size) {
      if (value.isNullAt(i)) {
        val intIndex = ((i >> 5) << 2) + offset
        val bitShift = i & 0x1f
        val oldBits = readInt(intIndex)
        val newBits = oldBits | (0x1 << bitShift)
        putInt(newBits, intIndex)
        assert(readInt(intIndex) == newBits)
      } else {
        put(value.get(i), struct.byteOffsets(i) + offset, struct.fields(i).typ)
      }
      i += 1
    }
  }

  private def put(value: Annotation, offset: Int, elementType: Type) {
    assert(value != null, s"got a null value of type ${ elementType } at offset $offset")
    elementType match {
      case TInt | TCall => putInt(value.asInstanceOf[Int], offset)
      case TLong => putLong(value.asInstanceOf[Long], offset)
      case TFloat => putFloat(value.asInstanceOf[Float], offset)
      case TDouble => putDouble(value.asInstanceOf[Double], offset)
      case TBoolean => putByte(value.asInstanceOf[Boolean].toByte, offset)
      case TString => putBinary(value.asInstanceOf[String].getBytes(), offset)
      case TArray(et) => putArray(value.asInstanceOf[IndexedSeq[_]], offset, et)
      case TSet(et) => putArray(value.asInstanceOf[Set[_]], offset, et)
      case t: TDict =>
        val m = value.asInstanceOf[Map[Any, Any]]
        dictKeyBuffer.clear()
        dictValueBuffer.clear()
        m.foreach { case (k, v) =>
            dictKeyBuffer += k
            dictValueBuffer += v
        }
        dictArrayEmulator.foreach(t.memStruct.typeCheck(_))
//        println(dictArrayEmulator)
        println(s"pre array write (offset $offset) = " + readInt(offset))
        putArray(dictArrayEmulator, offset, t.memStruct)
        println(s"post array write (offset $offset) = " + readInt(offset))
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
    putStruct(r, 0, t)
    val res = result()
    clear()
    res
  }

  private def clear() {
    java.util.Arrays.fill(mem, 0.toByte)
    initializeAppendPointer()
  }


  private def result(): UnsafeRow = {
    val memCopy = new Array[Byte](appendPointer)
    Platform.copyMemory(mem, Platform.BYTE_ARRAY_OFFSET, memCopy, Platform.BYTE_ARRAY_OFFSET, appendPointer)
    new UnsafeRow(memCopy, t, debug = debug)
  }
}

class UnsafeRow(mem: Array[Byte], t: TStruct, shiftOffset: Int = 0, debug: Boolean = false) extends Row {

  override def length: Int = t.size

  private def absolute(offset: Int): Int = Platform.BYTE_ARRAY_OFFSET + shiftOffset + offset

  private def readInt(offset: Int) = Platform.getInt(mem, absolute(offset))

  private def readLong(offset: Int) = Platform.getLong(mem, absolute(offset))

  private def readFloat(offset: Int) = Platform.getFloat(mem, absolute(offset))

  private def readDouble(offset: Int) = Platform.getDouble(mem, absolute(offset))

  private def readByte(offset: Int) = Platform.getByte(mem, absolute(offset))

  def readBinary(offset: Int): Array[Byte] = {
    val shift = readInt(offset)
    assert(shift > 0 && (shift & 0x3) == 0, s"invalid shift: $shift, from offset $offset (shift offset $shiftOffset)")

    val binStart = offset + shift
    val binLength = readInt(binStart)
    if (debug)
      println(s"reading type Binary of length $binLength from offset $offset (+$shiftOffset=${ offset + shiftOffset }) with shift $shift")
    val arr = new Array[Byte](binLength)
    Platform.copyMemory(mem, absolute(binStart + 4), arr, Platform.BYTE_ARRAY_OFFSET, binLength)

    arr
  }

  private def readArray(offset: Int, elementType: Type): IndexedSeq[Any] = {
    val shift = readInt(offset)
    assert(shift > 0 && (shift & 0x3) == 0, s"invalid shift: $shift, from offset $offset (shift offset $shiftOffset)")
    println(s"trying to read array with type ${elementType.toPrettyString(compact = true)} from offset $offset+$shift")

    val arrStart = offset + shift
    val arrLength = readInt(arrStart)
    val missingBytes = UnsafeAnnotations.nMissingBytes(arrLength)
    val elemsStart = UnsafeAnnotations.roundUpAlignment(arrStart + 4 + missingBytes + shiftOffset) - shiftOffset
    val eltSize = UnsafeAnnotations.arrayElementSize(elementType)
    if (debug)
      println(s"reading Array[${ elementType.toPrettyString(compact = true) }] of length $arrLength from $offset(+${ shiftOffset }+$shift=${ offset + shiftOffset + shift })")
    //    println(s"reading array at offset $offset with shift ${ shift }, has length $arrLength")

    //    println(s"t is $elementType")
    //    println(s"arrLength is $arrLength")

    val a = new Array[Any](arrLength)

    var i = 0
    while (i < arrLength) {

      val intIndex = (i >> 5) << 2
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