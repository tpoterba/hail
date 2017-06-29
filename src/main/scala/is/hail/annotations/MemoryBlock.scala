package is.hail.annotations

import org.apache.spark.unsafe.Platform

final class MemoryBlock(val mem: Array[Long]) {
  require(mem.length < (Integer.MAX_VALUE / 8), "too big")

  def loadInt(off: Int): Int = Platform.getInt(mem, Platform.LONG_ARRAY_OFFSET + off)

  def loadLong(off: Int): Long = Platform.getLong(mem, Platform.LONG_ARRAY_OFFSET + off)

  def loadFloat(off: Int): Float = Platform.getFloat(mem, Platform.LONG_ARRAY_OFFSET + off)

  def loadDouble(off: Int): Double = Platform.getDouble(mem, Platform.LONG_ARRAY_OFFSET + off)

  def loadByte(off: Int): Byte = Platform.getByte(mem, Platform.LONG_ARRAY_OFFSET + off)

  def loadBytes(off: Int, size: Int): Array[Byte] = {
    val a = new Array[Byte](size)
    Platform.copyMemory(mem, Platform.LONG_ARRAY_OFFSET + off, a, Platform.BYTE_ARRAY_OFFSET, size)
    a
  }

  def reallocate(size: Int): MemoryBlock = {
    if (mem.length * 8 < size) {
      val newMem = new Array[Long](math.min(Int.MaxValue, (size.toLong * 2 + 7) / 8).toInt)
      Platform.copyMemory(mem, Platform.LONG_ARRAY_OFFSET, newMem, Platform.LONG_ARRAY_OFFSET, mem.length * 8)
      new MemoryBlock(newMem)
    } else this
  }

  def copy(): MemoryBlock = new MemoryBlock(mem.clone())
}

final class Pointer(val mem: MemoryBlock, val memOffset: Int) {

  def loadInt(): Int = mem.loadInt(memOffset)

  def loadInt(off: Int): Int = mem.loadInt(off + memOffset)

  def loadLong(): Int = mem.loadInt(memOffset)

  def loadLong(off: Int): Long = mem.loadLong(off + memOffset)

  def loadFloat(): Float = mem.loadFloat(memOffset)

  def loadFloat(off: Int): Float = mem.loadFloat(off + memOffset)

  def loadDouble(): Double = mem.loadDouble(memOffset)

  def loadDouble(off: Int): Double = mem.loadDouble(off + memOffset)

  def loadByte(): Byte = mem.loadByte(memOffset)

  def loadByte(off: Int): Byte = mem.loadByte(off + memOffset)

  def loadBytes(size: Int): Array[Byte] = mem.loadBytes(memOffset, size)

  def loadBytes(off: Int, size: Int): Array[Byte] = mem.loadBytes(off + memOffset, size)

  def offset(off: Int): Pointer = new Pointer(mem, memOffset + off)

  def copy(): Pointer = new Pointer(mem.copy(), memOffset)
}

object MemoryBuffer {

  def apply(): MemoryBuffer = new MemoryBuffer(new MemoryBlock(new Array[Long](8)))

  def apply(size: Int): MemoryBuffer = new MemoryBuffer(new MemoryBlock(new Array[Long]((size + 7) / 8)))
}

final class MemoryBuffer(private
var mb: MemoryBlock) {
  var offset: Int = 0

  def loadInt(off: Int): Int = mb.loadInt(off)

  def loadLong(off: Int): Long = mb.loadLong(off)

  def loadFloat(off: Int): Float = mb.loadFloat(off)

  def loadDouble(off: Int): Double = mb.loadDouble(off)

  def loadByte(off: Int): Byte = mb.loadByte(off)

  def loadBytes(off: Int, size: Int): Array[Byte] = mb.loadBytes(off, size)

  def storeInt(off: Int, i: Int) {
    assert((off & 0x3) == 0, s"invalid int offset: $off")
    assert(off < (offset - 4))
    Platform.putInt(mb.mem, Platform.LONG_ARRAY_OFFSET + off, i)
  }

  def storeLong(off: Int, l: Long) {
    assert((off & 0x7) == 0, s"invalid long offset: $off")
    assert(off < (offset - 8))
    Platform.putLong(mb.mem, Platform.LONG_ARRAY_OFFSET + off, l)
  }

  def storeFloat(off: Int, f: Float) {
    assert((off & 0x3) == 0, s"invalid float offset: $off")
    assert(off < (offset - 4))
    Platform.putFloat(mb.mem, Platform.LONG_ARRAY_OFFSET + off, f)
  }

  def storeDouble(off: Int, d: Double) {
    assert((off & 0x7) == 0, s"invalid double offset: $off")
    assert(off < (offset - 8))
    Platform.putDouble(mb.mem, Platform.LONG_ARRAY_OFFSET + off, d)
  }

  def storeByte(off: Int, b: Byte) {
    assert(off < (offset - 1), s"tried to write to $off > $offset")
    Platform.putByte(mb.mem, Platform.LONG_ARRAY_OFFSET + off, b)
  }

  def storeBytes(off: Int, bytes: Array[Byte]) {
    assert(off < (offset - bytes.length))
    Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, mb.mem, Platform.LONG_ARRAY_OFFSET + off, bytes.length)
  }

  def appendInt(i: Int) {
    align(4)
    mb = mb.reallocate(offset + 4)
    Platform.putInt(mb.mem, Platform.LONG_ARRAY_OFFSET + offset, i)
    offset += 4
  }

  def appendLong(l: Long) {
    align(8)
    mb = mb.reallocate(offset + 8)
    Platform.putLong(mb.mem, Platform.LONG_ARRAY_OFFSET + offset, l)
    offset += 8
  }

  def appendFloat(f: Float) {
    align(4)
    mb = mb.reallocate(offset + 4)
    Platform.putFloat(mb.mem, Platform.LONG_ARRAY_OFFSET + offset, f)
    offset += 4
  }

  def appendDouble(d: Double) {
    align(8)
    mb = mb.reallocate(offset + 8)
    Platform.putDouble(mb.mem, Platform.LONG_ARRAY_OFFSET + offset, d)
    offset += 8
  }

  def appendByte(b: Byte) {
    mb = mb.reallocate(offset + 1)
    Platform.putByte(mb.mem, Platform.LONG_ARRAY_OFFSET + offset, b)
    offset += 1
  }

  def appendBytes(bytes: Array[Byte]) {
    mb = mb.reallocate(offset + bytes.length)
    Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, mb.mem, Platform.LONG_ARRAY_OFFSET + offset, bytes.length)
    offset += bytes.length
  }

  def allocate(nBytes: Int): Int = {
    val currentOffset = offset
    mb = mb.reallocate(offset + nBytes)
    offset += nBytes
    currentOffset
  }

  def align(alignment: Int) {
    alignment match {
      case 1 =>
      case 4 =>
        if ((offset & 0x3) != 0)
          offset += (4 - (offset & 0x7))
      case 8 =>
        if ((offset & 0x7) != 0)
          offset += (8 - (offset & 0x7))
      case _ => throw new AssertionError(s"tried to align to $alignment bytes")
    }
  }

  def clear() {
    offset = 0
  }

  def result(): MemoryBlock = {
    val reqLength = (offset + 7) / 8
    val arr = new Array[Long](reqLength)
    Platform.copyMemory(mb.mem, Platform.LONG_ARRAY_OFFSET, arr, Platform.LONG_ARRAY_OFFSET, offset)
    new MemoryBlock(arr)
  }
}

/*
PROBLEMS
array offsets are stored absolute, but pointer modifies everything by its own offset

clear? result? What do we want to do here?
 */