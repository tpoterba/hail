package is.hail.annotations

import java.io.DataInputStream

import is.hail.HailContext
import is.hail.expr.TStruct
import is.hail.utils.{SerializableHadoopConfiguration, _}
import net.jpountz.lz4.{LZ4BlockInputStream, LZ4Factory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.unsafe.Platform
import org.apache.spark.{Partition, TaskContext}

case class UnsafeRowStoreRDDPartition(pathBase: String, index: Int) extends Partition

class UnsafeRowStoreRDD(hc: HailContext, pathBase: String, schema: TStruct, nPartitions: Int) extends RDD[Row](hc.sc, Nil) {
  private val schemaBc = hc.sc.broadcast(schema)

  override def getPartitions: Array[Partition] = (0 until nPartitions)
    .map { i => UnsafeRowStoreRDDPartition(pathBase, i): Partition }.toArray

  private val sConf = hc.sc.broadcast(new SerializableHadoopConfiguration(hc.hadoopConf))

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val rawIn = sConf.value.value.unsafeReader(pathBase + s"/part-${ split.index }")
    val decompReader = new LZ4BlockInputStream(rawIn, LZ4Factory.fastestInstance().fastDecompressor())
    val in = new DataInputStream(decompReader)

    new Iterator[Row] {

      private var checked = false
      private var nextRowLength = -1

      var buffer: Array[Byte] = new Array[Byte](1024)

      private def check() {
        if (!checked) {
          nextRowLength = in.readInt()
          checked = true
        }
      }

      def hasNext: Boolean = {
        check()

        if (nextRowLength >= 0)
          true
        else {
          assert(nextRowLength == -1, s"invalid record size: $nextRowLength")
          in.close()
          false
        }
      }

      private def reallocate(n: Int) {
        if (buffer.length < n)
          buffer = new Array[Byte](2 * n)
      }

      def next(): Row = {
        if (!hasNext)
          throw new NoSuchElementException("next on empty iterator")
        checked = false
        var nRead = 0
        reallocate(nextRowLength)
        while (nRead < nextRowLength) {
          val bytesRead = in.read(buffer, nRead, nextRowLength - nRead)
          assert(bytesRead >= 0)
          nRead += bytesRead
        }

        val newMemoryBlock = new MemoryBlock(new Array[Long]((nextRowLength + 7) / 8))

        Platform.copyMemory(buffer, Platform.BYTE_ARRAY_OFFSET, newMemoryBlock.mem, Platform.LONG_ARRAY_OFFSET, nextRowLength)

        val r = new UnsafeRow(schema, newMemoryBlock, 0, debug = false)
        r
      }
    }
  }
}

