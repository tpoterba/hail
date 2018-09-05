package is.hail.annotations

import is.hail.expr.ordering.ExtendedOrdering
import is.hail.expr.types.{TArray, TDict, TSet}
import is.hail.expr.types.physical._
import is.hail.utils._
import is.hail.variant.Locus
import org.apache.spark.sql.Row

class RegionValueBuilder(var region: Region) {
  def this() = this(null)

  var start: Long = _
  var root: PType = _

  val typestk = new ArrayStack[PType]()
  val indexstk = new ArrayStack[Int]()
  val offsetstk = new ArrayStack[Long]()
  val elementsOffsetstk = new ArrayStack[Long]()

  def inactive: Boolean = root == null && typestk.isEmpty && offsetstk.isEmpty && elementsOffsetstk.isEmpty && indexstk.isEmpty

  def clear(): Unit = {
    root = null
    typestk.clear()
    offsetstk.clear()
    elementsOffsetstk.clear()
    indexstk.clear()
  }

  def set(newRegion: Region) {
    assert(inactive)
    region = newRegion
  }

  def currentOffset(): Long = {
    if (typestk.isEmpty)
      start
    else {
      val i = indexstk.top
      typestk.top match {
        case t: PConstructableStruct =>
          offsetstk.top + t.byteOffsets(i)
        case t: PConstructableArray =>
          elementsOffsetstk.top + i * t.elementByteSize
      }
    }
  }

  def currentType(): PType = {
    if (typestk.isEmpty)
      root
    else {
      typestk.top match {
        case t: PConstructableStruct =>
          val i = indexstk.top
          t.types(i)
        case t: PConstructableArray =>
          t.elementType
      }
    }
  }

  def start(newRoot: PType) {
    assert(inactive)
    root = newRoot
  }

  def allocateRoot() {
    assert(typestk.isEmpty)
    root match {
      case _: PPointer =>
      case _ =>
        start = region.allocate(root.alignment, root.byteSize)
    }
  }

  def end(): Long = {
    assert(root != null)
    root = null
    assert(inactive)
    start
  }

  def advance() {
    if (indexstk.nonEmpty)
      indexstk(0) = indexstk(0) + 1
  }

  /**
    * Unsafe unless the bytesize of every type being "advanced past" is size
    * 0. The primary use-case is when adding an array of hl.tstruct()
    * (i.e. empty structs).
    *
    **/
  def unsafeAdvance(i: Int) {
    if (indexstk.nonEmpty)
      indexstk(0) = indexstk(0) + i
  }

  def startBaseStruct(init: Boolean = true) {
    val t = currentType().asInstanceOf[PConstructableStruct]
    if (typestk.isEmpty)
      allocateRoot()

    val off = currentOffset()
    typestk.push(t)
    offsetstk.push(off)
    indexstk.push(0)

    if (init)
      t.clearMissingBits(region, off)
  }

  def endBaseStruct() {
    val t = typestk.top.asInstanceOf[PConstructableStruct]
    typestk.pop()
    offsetstk.pop()
    val last = indexstk.pop()
    assert(last == t.size)

    advance()
  }

  def startStruct(init: Boolean = true) {
    assert(currentType().isInstanceOf[PConstructableStruct])
    startBaseStruct(init)
  }

  def endStruct() {
    assert(typestk.top.isInstanceOf[PConstructableStruct])
    endBaseStruct()
  }

  def startTuple(init: Boolean = true) {
    assert(currentType().isInstanceOf[PConstructableStruct])
    startBaseStruct(init)
  }

  def endTuple() {
    assert(typestk.top.isInstanceOf[PConstructableStruct])
    endBaseStruct()
  }

  def startArray(length: Int, init: Boolean = true) {
    val t = currentType().asInstanceOf[PConstructableArray]
    val aoff = t.allocate(region, length)

    if (typestk.nonEmpty) {
      val off = currentOffset()
      region.storeAddress(off, aoff)
    } else
      start = aoff

    typestk.push(t)
    elementsOffsetstk.push(aoff + t.elementsOffset(length))
    indexstk.push(0)
    offsetstk.push(aoff)

    if (init)
      t.initialize(region, aoff, length)
  }

  def endArray() {
    val t = typestk.top.asInstanceOf[PConstructableArray]
    val aoff = offsetstk.top
    val length = t.loadLength(region, aoff)
    assert(length == indexstk.top)

    typestk.pop()
    offsetstk.pop()
    elementsOffsetstk.pop()
    indexstk.pop()

    advance()
  }

  def setFieldIndex(newI: Int) {
    assert(typestk.top.isInstanceOf[PConstructableStruct])
    indexstk(0) = newI
  }

  def setMissing() {
    val i = indexstk.top
    typestk.top match {
      case t: PConstructableStruct =>
        t.setFieldMissing(region, offsetstk.top, i)
      case t: PConstructableArray=>
        t.setElementMissing(region, offsetstk.top, i)
    }
    advance()
  }

  def addBoolean(b: Boolean) {
    assert(currentType() == PCanonicalBoolean)
    if (typestk.isEmpty)
      allocateRoot()
    val off = currentOffset()
    region.storeByte(off, b.toByte)
    advance()
  }

  def addInt(i: Int) {
    assert(currentType() == PCanonicalInt32)
    if (typestk.isEmpty)
      allocateRoot()
    val off = currentOffset()
    region.storeInt(off, i)
    advance()
  }

  def addLong(l: Long) {
    assert(currentType() == PCanonicalInt64)
    if (typestk.isEmpty)
      allocateRoot()
    val off = currentOffset()
    region.storeLong(off, l)
    advance()
  }

  def addFloat(f: Float) {
    assert(currentType() == PCanonicalFloat32)
    if (typestk.isEmpty)
      allocateRoot()
    val off = currentOffset()
    region.storeFloat(off, f)
    advance()
  }

  def addDouble(d: Double) {
    assert(currentType() == PCanonicalFloat64)
    if (typestk.isEmpty)
      allocateRoot()
    val off = currentOffset()
    region.storeDouble(off, d)
    advance()
  }

  def addBinary(bytes: Array[Byte]) {
    assert(currentType() == PCanonicalBinary)

    val boff = region.appendBinary(bytes)

    if (typestk.nonEmpty) {
      val off = currentOffset()
      region.storeAddress(off, boff)
    } else
      start = boff

    advance()
  }

  def addString(s: String) {
    addBinary(s.getBytes)
  }

  def addRow(r: Row) {
    val t = currentType().asInstanceOf[PConstructableStruct]
    assert(r != null)
    startBaseStruct()
    var i = 0
    while (i < t.size) {
      addAnnotation(r.get(i))
      i += 1
    }
    endBaseStruct()
  }

  def addField(t: PStruct, fromRegion: Region, fromOff: Long, i: Int) {
    if (t.isFieldDefined(fromRegion, fromOff, i))
      addRegionValue(t.types(i), fromRegion, t.loadField(fromRegion, fromOff, i))
    else
      setMissing()
  }

  def addField(t: PStruct, rv: RegionValue, i: Int) {
    addField(t, rv.region, rv.offset, i)
  }

  def skipFields(n: Int) {
    var i = 0
    while (i < n) {
      setMissing()
      i += 1
    }
  }

  def addAllFields(t: PStruct, fromRegion: Region, fromOff: Long) {
    var i = 0
    while (i < t.size) {
      addField(t, fromRegion, fromOff, i)
      i += 1
    }
  }

  def addAllFields(t: PStruct, fromRV: RegionValue) {
    addAllFields(t, fromRV.region, fromRV.offset)
  }

  def addFields(t: PStruct, fromRegion: Region, fromOff: Long, fieldIdx: Array[Int]) {
    var i = 0
    while (i < fieldIdx.length) {
      addField(t, fromRegion, fromOff, fieldIdx(i))
      i += 1
    }
  }

  def addFields(t: PStruct, fromRV: RegionValue, fieldIdx: Array[Int]) {
    addFields(t, fromRV.region, fromRV.offset, fieldIdx)
  }

  def addElement(t: PArray, fromRegion: Region, fromAOff: Long, i: Int) {
    if (t.isElementDefined(fromRegion, fromAOff, i))
      addRegionValue(t.elementType, fromRegion,
        t.elementOffsetInRegion(fromRegion, fromAOff, i))
    else
      setMissing()
  }

  def addElement(t: PArray, rv: RegionValue, i: Int) {
    addElement(t, rv.region, rv.offset, i)
  }

  def selectRegionValue(fromT: PStruct, fromFieldIdx: Array[Int], fromRV: RegionValue) {
    selectRegionValue(fromT, fromFieldIdx, fromRV.region, fromRV.offset)
  }

  def selectRegionValue(fromT: PStruct, fromFieldIdx: Array[Int], region: Region, offset: Long) {
    startStruct()
    addFields(fromT, region, offset, fromFieldIdx)
    endStruct()
  }

  def addRegionValue(t: PType, rv: RegionValue) {
    addRegionValue(t, rv.region, rv.offset)
  }

  def addRegionValue(t: PType, fromRegion: Region, fromOff: Long) {
    val toT = currentType()

    if (typestk.isEmpty) {
      if (region.eq(fromRegion)) {
        start = fromOff
        advance()
        return
      }

      allocateRoot()
    }

    val toOff = currentOffset()
    assert(typestk.nonEmpty || toOff == start)

    region.copyFrom(fromRegion, fromOff, toOff, t.byteSize)
    advance()
  }

  def addUnsafeRow(t: PStruct, ur: UnsafeRow) {
    assert(t == ur.t)
    addRegionValue(t, ur.region, ur.offset)
  }

  def addUnsafeArray(t: PArray, uis: UnsafeIndexedSeq) {
    assert(t == uis.t)
    addRegionValue(t, uis.region, uis.aoff)
  }

  def addAnnotation(a: Annotation) {
    if (a == null)
      setMissing()
    else
      currentType() match {
        case _: PBoolean => addBoolean(a.asInstanceOf[Boolean])
        case _: PInt32 => addInt(a.asInstanceOf[Int])
        case _: PInt64 => addLong(a.asInstanceOf[Long])
        case _: PFloat32 => addFloat(a.asInstanceOf[Float])
        case _: PFloat64 => addDouble(a.asInstanceOf[Double])
        case _: PString => addString(a.asInstanceOf[String])
        case _: PBinary => addBinary(a.asInstanceOf[Array[Byte]])

        case t: PArray =>
          t.virtualType match {
            case ta : TArray =>
              a match {
                case uis: UnsafeIndexedSeq if t == uis.t =>
                  addUnsafeArray(t, uis)

                case is: IndexedSeq[Annotation] =>
                  startArray(is.length)
                  var i = 0
                  while (i < is.length) {
                    addAnnotation(t.elementType, is(i))
                    i += 1
                  }
                  endArray()
              }
            case ts: TSet =>
              val s = a.asInstanceOf[Set[Annotation]]
                .toArray
                .sorted(ExtendedOrdering(ts.elementType).toOrdering)
              startArray(s.length)
              s.foreach { x => addAnnotation(t.elementType, x) }
              endArray()
          case td: TDict =>
            val m = a.asInstanceOf[Map[Annotation, Annotation]]
              .map { case (k, v) => Row(k, v) }
              .toArray
              .sorted(ExtendedOrdering(td.elementType).toOrdering)
            startArray(m.length)
            m.foreach { case Row(k, v) =>
              startStruct()
              addAnnotation(td.keyType, k)
              addAnnotation(td.valueType, v)
              endStruct()
            }
            endArray()
          }

        case t: PStruct =>
          a match {
            case ur: UnsafeRow if t == ur.t =>
              addUnsafeRow(t, ur)
            case r: Row =>
              addRow(r)
          }

        case t: PCall => addInt(a.asInstanceOf[Int])

        case t: PLocus =>
          val l = a.asInstanceOf[Locus]
          startStruct()
          addString(l.contig)
          addInt(l.position)
          endStruct()

        case t: PInterval =>
          val i = a.asInstanceOf[Interval]
          startStruct()
          addAnnotation(t.pointType, i.start)
          addAnnotation(t.pointType, i.end)
          addBoolean(i.includesStart)
          addBoolean(i.includesEnd)
          endStruct()
      }

  }

  def result(): RegionValue = RegionValue(region, start)
}
