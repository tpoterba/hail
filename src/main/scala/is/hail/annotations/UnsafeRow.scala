package is.hail.annotations

import java.lang.reflect.Constructor

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import is.hail.expr._
import is.hail.variant.{AltAllele, Genotype, Locus, Variant}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

object UnsafeRow {
  private val bcConstructor: Constructor[_] = {
    val torr = Class.forName("org.apache.spark.broadcast.TorrentBroadcast")
    torr.getDeclaredConstructor(classOf[AnyRef], classOf[Long], classOf[ClassTag[_]])
  }
  private val structClass: ClassTag[TStruct] = ClassTag.apply(TStruct.getClass)

  private val m = new java.util.HashMap[Long, Broadcast[TStruct]]

  def lookupBroadcast(id: Long): Broadcast[TStruct] = {
    if (m.containsKey(id))
      m.get(id)
    else {
      val tbc = bcConstructor.newInstance(
        null: AnyRef,
        id: java.lang.Long,
        UnsafeRow.structClass).asInstanceOf[Broadcast[TStruct]]
      m.put(id, tbc)
      tbc
    }
  }
}
class UnsafeRow(@transient var _t: TStruct, var mb: MemoryBlock, var mbOffset: Int, debug: Boolean = false) extends Row with KryoSerializable {


  private var schemaBc: Broadcast[TStruct] = _

  def t: TStruct = {
    if (_t != null)
      _t
    else if (schemaBc != null) {
      _t = schemaBc.value
      _t
    } else throw new RuntimeException("Serialized struct without setting broadcast schema")
  }

  def length: Int = _t.size

  private def readBinary(offset: Int): Array[Byte] = {
    val start = mb.loadInt(offset)
    assert(offset > 0 && (offset & 0x3) == 0, s"invalid binary start: $offset")
    val binLength = mb.loadInt(start)
    val b = mb.loadBytes(start + 4, binLength)
    if (debug)
      println(s"from absolute offset $start, read length ${ binLength }, bytes='${ new String(b) }'")

    b
  }

  private def readArray(offset: Int, t: Type): IndexedSeq[Any] = {
    val start = mb.loadInt(offset)
    if (debug)
      println(s"reading array from ${ offset }+${ mbOffset }=${ offset + mbOffset } -> $start")

    assert(start > 0 && (start & 0x3) == 0, s"invalid array start: $offset")

    val arrLength = mb.loadInt(start)
    val missingBytes = (arrLength + 7) / 8
    val elemsStart = UnsafeUtils.roundUpAlignment(start + 4 + missingBytes, t.alignment)
    val eltSize = UnsafeUtils.arrayElementSize(t)

    if (debug)
      println(s"reading array from absolute offset $start. Length=$arrLength, elemsStart=$elemsStart, elemSize=$eltSize")

    val a = new Array[Any](arrLength)

    var i = 0
    while (i < arrLength) {

      val byteIndex = i / 8
      val bitShift = i & 0x7
      val missingByte = mb.loadByte(start + 4 + byteIndex)
      val isMissing = (missingByte & (0x1 << bitShift)) != 0

      if (!isMissing)
        a(i) = read(elemsStart + i * eltSize, t)

      i += 1
    }

    a
  }

  private def readStruct(offset: Int, t: TStruct): UnsafeRow = {
    if (debug)
      println(s"reading struct $t from offset ${ offset }+${ mbOffset }=${ offset + mbOffset }")
    new UnsafeRow(t, mb, offset, debug)
  }

  private def read(offset: Int, t: Type): Any = {
    t match {
      case TBoolean =>
        val b = mb.loadByte(offset)
        assert(b == 0 || b == 1, s"invalid boolean byte $b from offset $offset")
        b == 1
      case TInt | TCall => mb.loadInt(offset)
      case TLong => mb.loadLong(offset)
      case TFloat => mb.loadFloat(offset)
      case TDouble => mb.loadDouble(offset)
      case TArray(elementType) => readArray(offset, elementType)
      case TSet(elementType) => readArray(offset, elementType).toSet
      case TString => new String(readBinary(offset))
      case td: TDict =>
        readArray(offset, td.elementType).asInstanceOf[IndexedSeq[Row]].map(r => (r.get(0), r.get(1))).toMap
      case struct: TStruct =>
        readStruct(offset, struct)
      case TVariant => Variant.fromRow(readStruct(offset, TVariant.representation))
      case TLocus => Locus.fromRow(readStruct(offset, TLocus.representation))
      case TAltAllele => AltAllele.fromRow(readStruct(offset, TAltAllele.representation))
      case TGenotype => Genotype.fromRow(readStruct(offset, TGenotype.representation))
      case TInterval => Locus.intervalFromRow(readStruct(offset, TInterval.representation))
      case _ => ???
    }
  }

  private def assertDefined(i: Int) {
    if (isNullAt(i))
      throw new NullPointerException(s"null value at index $i")
  }

  def get(i: Int): Any = {
    val offset = _t.byteOffsets(i)
    if (isNullAt(i))
      null
    else
      read(mbOffset + offset, t.fields(i).typ)
  }

  def copy(): Row = new UnsafeRow(t, mb.copy(), mbOffset, debug)

  override def getInt(i: Int): Int = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    mb.loadInt(mbOffset + offset)
  }

  override def getLong(i: Int): Long = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    mb.loadLong(mbOffset + offset)
  }

  override def getFloat(i: Int): Float = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    mb.loadFloat(mbOffset + offset)
  }

  override def getDouble(i: Int): Double = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    mb.loadDouble(mbOffset + offset)
  }

  override def getBoolean(i: Int): Boolean = {
    getByte(i) == 1
  }

  override def getByte(i: Int): Byte = {
    assertDefined(i)
    val offset = t.byteOffsets(i)
    mb.loadByte(mbOffset + offset)
  }

  override def isNullAt(i: Int): Boolean = {
    if (i < 0 || i >= _t.size)
      throw new IndexOutOfBoundsException(i.toString)
    val byteIndex = i / 8
    val bitShift = i & 0x7
    (mb.loadByte(mbOffset + byteIndex) & (0x1 << bitShift)) != 0
  }

  def isCanonical: Boolean = mbOffset == 0

  // methods for serialization
  //  def writeToRowStore(out: DataOutputStream) {
  //    val arr = new Array[Byte](ptr.mem.nBytes)
  //    Platform.copyMemory(ptr.mem.arr, Platform.LONG_ARRAY_OFFSET, arr, Platform.BYTE_ARRAY_OFFSET, ptr.mem.nBytes)
  //    out.writeInt(ptr.mem.nBytes)
  //    out.write(arr)
  //
  //    //    var i = 0
  //    //    while (i < ptr.mem.arr.length) {
  //    //      out.writeLong(ptr.mem.arr(i))
  //    //      i += 1
  //    //    }
  //  }

  def setBroadcast(schemaBc: Broadcast[TStruct]) {
    this.schemaBc = schemaBc
  }

  override def write(kryo: Kryo, output: Output) {
    require(schemaBc != null, "tried to serialize before setting broadcast schema")
    require(isCanonical, s"tried to serialize a non-canonical row: offset=${ mbOffset }")
    output.writeLong(schemaBc.id)
    MemoryBlock.writeKryo(mb, output)
  }

  override def read(kryo: Kryo, input: Input) {
    val broadcastID = input.readLong()
    schemaBc = UnsafeRow.lookupBroadcast(broadcastID)
    _t = schemaBc.value
    val length = input.readInt()
    assert(length >= 0, s"invalid length: $length")
    mb = MemoryBlock.readKryo(input)
    mbOffset = 0
  }
}
