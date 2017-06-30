package is.hail.annotations

import is.hail.SparkSuite
import is.hail.check._
import is.hail.check.Arbitrary._
import is.hail.expr._
import is.hail.variant.{Genotype, Locus}
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class UnsafeSuite extends SparkSuite {

  @Test def testMemoryBuffer() {
    val buff = MemoryBuffer()

    buff.appendLong(124L)
    buff.appendByte(2)
    buff.appendByte(1)
    buff.appendByte(4)
    buff.appendInt(1234567)
    buff.appendDouble(1.1)

    assert(buff.loadLong(0) == 124L)
    assert(buff.loadByte(8) == 2)
    assert(buff.loadByte(9) == 1)
    assert(buff.loadByte(10) == 4)
    assert(buff.loadInt(12) == 1234567)
    assert(buff.loadDouble(16) == 1.1)
  }


  def genStructTypeAndNonMissingValue: Gen[(TStruct, Annotation)] = for {
    (x, y) <- Gen.squareOfAreaAtMostSize
    t <- Type.genStruct.resize(x)
    v <- t.genNonmissingValue.resize(y)
  } yield (t, v)

  @Test def testRandom() {
    Prop.forAll(genStructTypeAndNonMissingValue.filter(_._2 != null)) { case (t, a) =>
      println(t.toPrettyString(compact = true))
        val urb = new UnsafeRowBuilder(t, debug = true)
        val unsafeRow = urb.convert(a.asInstanceOf[Row])
        val p = unsafeRow == a
        if (!p) {
          println(
            s"""IN:  $a
               |OUT: $unsafeRow""".stripMargin)

          println(unsafeRow.get(2).asInstanceOf[Locus].contig)
          println(unsafeRow.get(2).asInstanceOf[Locus].contig.length)
//          for (i <- 0 until t.size)
//            println(s"field $i is ${unsafeRow.get(i)}")
        }
        p

    }.check()
  }

//  @Test def test2() {
//    println(Locus.intervalExpandedType.byteOffsets)
//    println(Genotype.expandedType.byteOffsets.toSeq)
//    println(Genotype.expandedType.byteSize)
//  }
}
