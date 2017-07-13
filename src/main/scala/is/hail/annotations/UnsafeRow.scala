package is.hail.annotations

import is.hail.expr.{TAltAllele, TArray, TBoolean, TCall, TDict, TDouble, TFloat, TGenotype, TInt, TInterval, TLocus, TLong, TSet, TString, TStruct, TVariant, Type}
import is.hail.variant.{AltAllele, Genotype, Locus, Variant}
import org.apache.spark.sql.Row

class UnsafeRow(@transient var t: TStruct, var ptr: Pointer, debug: Boolean = false) extends Row {

  def length: Int = t.size

  private def readBinaryAbsolute(offset: Int): Array[Byte] = {
    val start = ptr.mb.loadInt(offset)

    readBytesAbsolute(start)
  }

  private def readBytesAbsolute(offset: Int): Array[Byte] = {
    assert(offset > 0 && (offset & 0x3) == 0, s"invalid binary start: $offset")
    val binLength = ptr.mb.loadInt(offset)
    val b = ptr.mb.loadBytes(offset + 4, binLength)
    if (debug)
      println(s"from absolute offset $offset, read length ${ binLength }, bytes='${ new String(b) }'")

    b
  }

  private def readBinary(offset: Int): Array[Byte] = {
    val start = ptr.loadInt(offset)
    if (debug)
      println(s"reading binary from initial offset $offset(+${ ptr.offset }=${ ptr.offset + offset }), going to $start")

    readBytesAbsolute(start)
  }

  private def readArrayElems(offset: Int, t: Type): IndexedSeq[Any] = {
    assert(offset > 0 && (offset & 0x3) == 0, s"invalid array start: $offset")

    val arrLength = ptr.mb.loadInt(offset)
    val missingBytes = (arrLength + 7) / 8
    val elemsStart = UnsafeUtils.roundUpAlignment(offset + 4 + missingBytes, t.alignment)
    val eltSize = UnsafeUtils.arrayElementSize(t)

    if (debug)
      println(s"reading array from absolute offset $offset. Length=$arrLength, elemsStart=$elemsStart, elemSize=$eltSize")

    val a = new Array[Any](arrLength)

    var i = 0
    while (i < arrLength) {

      val byteIndex = i / 8
      val bitShift = i & 0x7
      val missingByte = ptr.mb.loadByte(offset + 4 + byteIndex)
      val isMissing = (missingByte & (0x1 << bitShift)) != 0

      if (!isMissing)
        a(i) = readAbsolute(elemsStart + i * eltSize, t)

      i += 1
    }

    a
  }

  private def readArrayAbsolute(offset: Int, t: Type): IndexedSeq[Any] = {
    val start = ptr.mb.loadInt(offset)
    if (debug)
      println(s"reading array from $offset -> ${ start }")
    readArrayElems(start, t)
  }


  private def readArray(offset: Int, t: Type): IndexedSeq[Any] = {
    val start = ptr.loadInt(offset)
    if (debug)
      println(s"reading array from ${ offset }+${ ptr.offset }=${ offset + ptr.offset } -> $start")
    readArrayElems(start, t)
  }

  private def readStructAbsolute(offset: Int, t: TStruct): UnsafeRow = {
    if (debug)
      println(s"reading struct $t from offset ${ offset }")
    new UnsafeRow(t, new Pointer(ptr.mb, offset), debug)
  }

  private def readStruct(offset: Int, t: TStruct): UnsafeRow = {
    if (debug)
      println(s"reading struct $t from offset ${ offset }+${ ptr.offset }=${ offset + ptr.offset }")
    new UnsafeRow(t, new Pointer(ptr.mb, ptr.offset + offset), debug)
  }

  private def readAbsolute(offset: Int, t: Type): Any = {
    t match {
      case TBoolean =>
        val b = ptr.mb.loadByte(offset)
        assert(b == 0 || b == 1, s"invalid boolean byte $b from offset $offset")
        b == 1
      case TInt | TCall => ptr.mb.loadInt(offset)
      case TLong => ptr.mb.loadLong(offset)
      case TFloat => ptr.mb.loadFloat(offset)
      case TDouble => ptr.mb.loadDouble(offset)
      case TArray(elementType) => readArrayAbsolute(offset, elementType)
      case TSet(elementType) => readArrayAbsolute(offset, elementType).toSet
      case TString => new String(readBinaryAbsolute(offset))
      case td: TDict =>
        readArrayAbsolute(offset, td.elementType).asInstanceOf[IndexedSeq[Row]].map(r => (r.get(0), r.get(1))).toMap
      case struct: TStruct =>
        if (struct.size == 0)
          Annotation.empty
        else
          readStructAbsolute(offset, struct)
      case TVariant => Variant.fromRow(readStructAbsolute(offset, TVariant.representation.asInstanceOf[TStruct]))
      case TLocus => Locus.fromRow(readStructAbsolute(offset, TLocus.representation.asInstanceOf[TStruct]))
      case TAltAllele => AltAllele.fromRow(readStructAbsolute(offset, TAltAllele.representation.asInstanceOf[TStruct]))
      case TGenotype => Genotype.fromRow(readStructAbsolute(offset, TGenotype.representation.asInstanceOf[TStruct]))
      case TInterval => Locus.intervalFromRow(readStructAbsolute(offset, TInterval.representation.asInstanceOf[TStruct]))

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
      case TArray(elementType) => readArray(offset, elementType)
      case TSet(elementType) => readArray(offset, elementType).toSet
      case TString => new String(readBinary(offset))
      case td: TDict =>
        readArray(offset, td.elementType).asInstanceOf[IndexedSeq[Row]].map(r => (r.get(0), r.get(1))).toMap
      case struct: TStruct =>
        if (struct.size == 0)
          Annotation.empty
        else
          readStruct(offset, struct)
      case TVariant => Variant.fromRow(readStruct(offset, TVariant.representation.asInstanceOf[TStruct]))
      case TLocus => Locus.fromRow(readStruct(offset, TLocus.representation.asInstanceOf[TStruct]))
      case TAltAllele => AltAllele.fromRow(readStruct(offset, TAltAllele.representation.asInstanceOf[TStruct]))
      case TGenotype => Genotype.fromRow(readStruct(offset, TGenotype.representation.asInstanceOf[TStruct]))
      case TInterval => Locus.intervalFromRow(readStruct(offset, TInterval.representation.asInstanceOf[TStruct]))

      case _ => ???
    }
  }

  private def assertDefined(i: Int) {
    if (isNullAt(i))
      throw new NullPointerException(s"null value at index $i")
  }

  def get(i: Int): Any = {
    val offset = t.byteOffsets(i)
    if (isNullAt(i))
      null
    else
      read(offset, t.fields(i).typ)
  }

  def copy(): Row = new UnsafeRow(t, ptr.copy(), debug)

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

  override def isNullAt(i: Int): Boolean = {
    if (i < 0 || i >= t.size)
      throw new IndexOutOfBoundsException(i.toString)
    val byteIndex = i / 8
    val bitShift = i & 0x7
    (ptr.loadByte(byteIndex) & (0x1 << bitShift)) != 0
  }
}
