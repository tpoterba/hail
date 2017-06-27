package is.hail.annotations

import org.apache.spark.unsafe.Platform

final class MemoryBlock(mem: Array[Long]) {
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
}

final class Pointer(mem: MemoryBlock, memOffset: Int) {
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
}

final class MemoryBuffer(var mem: MemoryBlock, offset: Int) {
  var cursor: Int = 0

  def loadInt(off: Int): Int = mem.loadInt(off)

  def loadLong(off: Int): Long = mem.loadLong(off)

  def loadFloat(off: Int): Float = mem.loadFloat(off)

  def loadDouble(off: Int): Double = mem.loadDouble(off)

  def loadByte(off: Int): Byte = mem.loadByte(off)

  def loadBytes(off: Int, size: Int): Array[Byte] = mem.loadBytes(off, size)

  def appendInt(i: Int) {
    align(4)
    reallocate(cursor + 4)
    Platform.putInt(mem, Platform.LONG_ARRAY_OFFSET + cursor, i)
    cursor += 4
  }

  def appendLong(l: Long) {
    align(8)
    reallocate(cursor + 8)
    Platform.putLong(mem, Platform.LONG_ARRAY_OFFSET + cursor, l)
    cursor += 8
  }

  def appendFloat(f: Float) {
    align(4)
    mem = mem.reallocate(cursor + 4)
    Platform.putFloat(mem, Platform.LONG_ARRAY_OFFSET + cursor, f)
    cursor += 4
  }

  def appendDouble(d: Double) {
    align(8)
    reallocate(cursor + 8)
    Platform.putDouble(mem, Platform.LONG_ARRAY_OFFSET + cursor, d)
    cursor += 8
  }

  def appendByte(b: Byte) {
    reallocate(cursor + 1)
    Platform.putByte(mem, Platform.LONG_ARRAY_OFFSET + cursor, b)
    cursor += 1
  }

  def appendBytes(bytes: Array[Byte]) {
    reallocate(cursor + bytes.length)
    Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, mem, Platform.LONG_ARRAY_OFFSET + cursor, bytes.length)
    cursor += bytes.length
  }

  def reallocate(nBytes: Int) {
    mem.reallocate(nBytes)
  }

  def align(alignment: Int) {
    alignment match {
      case 8 =>
        if ((cursor & 0x7) != 0)
          cursor += (8 - (cursor & 0x7))
      case 4 =>
        if ((cursor & 0x3) != 0)
          cursor += (8 - (cursor & 0x7))
      case _ => throw new AssertionError(s"tried to align to $alignment bytes")
    }
  }
}

/*
What do we want to write?

t.getField(i,


 */