package is.hail.annotations

import java.io.{ObjectInputStream, ObjectOutputStream}

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import is.hail.expr.types.{TArray, TDict, TSet}
import is.hail.expr.types.physical._
import is.hail.utils._
import is.hail.variant.{Locus, RGBase}
import org.apache.spark.sql.Row
import sun.reflect.generics.reflectiveObjects.NotImplementedException

trait UnKryoSerializable extends KryoSerializable {
  def write(kryo: Kryo, output: Output): Unit = {
    throw new NotImplementedException()
  }

  def read(kryo: Kryo, input: Input): Unit = {
    throw new NotImplementedException()
  }
}

object UnsafeIndexedSeq {
  def apply(t: PArray, elements: Array[RegionValue]): UnsafeIndexedSeq = {
    val region = Region()
    val rvb = new RegionValueBuilder(region)
    rvb.start(t)
    rvb.startArray(elements.length)
    var i = 0
    while (i < elements.length) {
      rvb.addRegionValue(t.elementType, elements(i))
      i += 1
    }
    rvb.endArray()

    new UnsafeIndexedSeq(t, region, rvb.end())
  }

  def apply(t: PArray, a: IndexedSeq[Annotation]): UnsafeIndexedSeq = {
    val region = Region()
    val rvb = new RegionValueBuilder(region)
    rvb.start(t)
    rvb.startArray(a.length)
    var i = 0
    while (i < a.length) {
      rvb.addAnnotation(t.elementType, a(i))
      i += 1
    }
    rvb.endArray()
    new UnsafeIndexedSeq(t, region, rvb.end())
  }

  def empty(t: PArray): UnsafeIndexedSeq = {
    val region = Region()
    val rvb = new RegionValueBuilder(region)
    rvb.start(t)
    rvb.startArray(0)
    rvb.endArray()
    new UnsafeIndexedSeq(t, region, rvb.end())
  }
}

class UnsafeIndexedSeq(
  var t: PArray,
  var region: Region, var aoff: Long) extends IndexedSeq[Annotation] with UnKryoSerializable {

  var length: Int = t.loadLength(region, aoff)

  def apply(i: Int): Annotation = {
    if (i < 0 || i >= length)
      throw new IndexOutOfBoundsException(i.toString)
    if (t.isElementDefined(region, aoff, i)) {
      UnsafeRow.read(t.elementType, region, t.loadElement(region, aoff, length, i))
    } else
      null
  }

  override def toString: String = s"[${ this.mkString(",") }]"
}

object UnsafeRow {
  def readBinary(region: Region, boff: Long): Array[Byte] = {
    val binLength = PCanonicalBinary.loadLength(region, boff)
    region.loadBytes(PCanonicalBinary.bytesOffset(boff), binLength)
  }

  def readString(region: Region, boff: Long): String = PCanonicalString.loadString(region, boff)

  def readArray(t: PArray, region: Region, aoff: Long): IndexedSeq[Any] =
    new UnsafeIndexedSeq(t, region, aoff)

  def readBaseStruct(t: PStruct, region: Region, offset: Long): UnsafeRow =
    new UnsafeRow(t, region, offset)

  def readLocus(t: PLocus, region: Region, offset: Long): Locus = {
    Locus(
      t.loadContig(region, offset),
      t.loadPosition(region, offset))
  }

  def read(t: PType, region: Region, offset: Long): Any = {
    t match {
      case t: PBoolean => t.load(region, offset)
      case t: PInt32 => t.load(region, offset)
      case t: PCall => t.load(region, offset)
      case t: PInt64 => region.loadLong(offset)
      case t: PFloat32 => region.loadFloat(offset)
      case t: PFloat64 => region.loadDouble(offset)
      case t: PArray =>
        val a = readArray(t, region, offset)
        t.virtualType match {
          case _: TArray => a
          case _: TSet => a.toSet
          case _: TDict => a.asInstanceOf[IndexedSeq[Row]].map(r => (r.get(0), r.get(1))).toMap
        }
      case t: PString => readString(region, offset)
      case t: PBinary => readBinary(region, offset)
      case t: PStruct => readBaseStruct(t, region, offset)
      case x: PLocus => readLocus(x, region, offset)
      case x: PInterval =>
        Interval(
          if (x.startDefined(region, offset)) read(x.pointType, region, x.loadStart(region, offset)) else null,
          if (x.endDefined(region, offset)) read(x.pointType, region, x.loadEnd(region, offset)) else null,
          x.includesStart(region, offset),
          x.includesEnd(region, offset)
        )
    }
  }
}

class UnsafeRow(var t: PStruct,
  var region: Region, var offset: Long) extends Row with UnKryoSerializable {

  def this(t: PStruct, rv: RegionValue) = this(t, rv.region, rv.offset)

  def this(t: PStruct) = this(t, null, 0)

  def this() = this(null, null, 0)

  def set(newRegion: Region, newOffset: Long) {
    region = newRegion
    offset = newOffset
  }

  def set(rv: RegionValue): Unit = set(rv.region, rv.offset)

  def length: Int = t.size

  private def assertDefined(i: Int) {
    if (isNullAt(i))
      throw new NullPointerException(s"null value at index $i")
  }

  def get(i: Int): Any = {
    if (isNullAt(i))
      null
    else
      UnsafeRow.read(t.types(i), region, t.loadField(region, offset, i))
  }

  def copy(): Row = new UnsafeRow(t, region, offset)

  def pretty(): String = region.pretty(t, offset)

  override def getInt(i: Int): Int = {
    assertDefined(i)
    region.loadInt(t.loadField(region, offset, i))
  }

  override def getLong(i: Int): Long = {
    assertDefined(i)
    region.loadLong(t.loadField(region, offset, i))
  }

  override def getFloat(i: Int): Float = {
    assertDefined(i)
    region.loadFloat(t.loadField(region, offset, i))
  }

  override def getDouble(i: Int): Double = {
    assertDefined(i)
    region.loadDouble(t.loadField(region, offset, i))
  }

  override def getBoolean(i: Int): Boolean = {
    assertDefined(i)
    region.loadBoolean(t.loadField(region, offset, i))
  }

  override def getByte(i: Int): Byte = {
    assertDefined(i)
    region.loadByte(t.loadField(region, offset, i))
  }

  override def isNullAt(i: Int): Boolean = {
    if (i < 0 || i >= t.size)
      throw new IndexOutOfBoundsException(i.toString)
    !t.isFieldDefined(region, offset, i)
  }

  private def writeObject(s: ObjectOutputStream): Unit = {
    throw new NotImplementedException()
  }

  private def readObject(s: ObjectInputStream): Unit = {
    throw new NotImplementedException()
  }
}

object SafeRow {
  def apply(t: PStruct, region: Region, off: Long): Row = {
    Annotation.copy(t.virtualType, new UnsafeRow(t, region, off)).asInstanceOf[Row]
  }

  def apply(t: PStruct, rv: RegionValue): Row = SafeRow(t, rv.region, rv.offset)

  def selectFields(t: PStruct, region: Region, off: Long)(selectIdx: Array[Int]): Row = {
    val fullRow = new UnsafeRow(t, region, off)
    Row(selectIdx.map(i => Annotation.copy(t.types(i).virtualType, fullRow.get(i))): _*)
  }

  def selectFields(t: PStruct, rv: RegionValue)(selectIdx: Array[Int]): Row =
    SafeRow.selectFields(t, rv.region, rv.offset)(selectIdx)

  def read(t: PType, region: Region, off: Long): Annotation =
    Annotation.copy(t.virtualType, UnsafeRow.read(t, region, off))

  def read(t: PType, rv: RegionValue): Annotation =
    read(t, rv.region, rv.offset)
}

object SafeIndexedSeq {
  def apply(t: PArray, region: Region, off: Long): IndexedSeq[Annotation] =
    Annotation.copy(t.virtualType, new UnsafeIndexedSeq(t, region, off))
      .asInstanceOf[IndexedSeq[Annotation]]

  def apply(t: PArray, rv: RegionValue): IndexedSeq[Annotation] =
    apply(t, rv.region, rv.offset)
}

class KeyedRow(var row: Row, keyFields: Array[Int]) extends Row {
  def length: Int = row.size

  def get(i: Int): Any = row.get(keyFields(i))

  def copy(): Row = new KeyedRow(row, keyFields)

  def set(newRow: Row): KeyedRow = {
    row = newRow
    this
  }
}
