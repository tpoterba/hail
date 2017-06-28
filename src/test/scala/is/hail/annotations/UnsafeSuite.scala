package is.hail.annotations

import is.hail.SparkSuite
import is.hail.check._
import is.hail.check.Arbitrary._
import is.hail.expr._
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class UnsafeSuite extends SparkSuite {

  @Test def testMemoryBlock() {
    val buff = MemoryBuffer()

    buff.appendLong(124L)
    buff.appendByte(2)
    buff.appendByte(1)
    buff.appendByte(4)
    buff.appendInt(1234567)
    buff.appendDouble(1.1)

    val mb = buff.mb
    assert(mb.loadLong(0) == 124L)
    assert(mb.loadByte(8) == 2)
    assert(mb.loadByte(9) == 1)
    assert(mb.loadByte(10) == 4)
    assert(mb.loadInt(12) == 1234567)
    assert(mb.loadDouble(16) == 1.1)
  }


  def genStructTypeAndNonMissingValue: Gen[(TStruct, Annotation)] = for {
    (x, y) <- Gen.squareOfAreaAtMostSize
    t <- Type.genStruct.resize(x)
    v <- t.genNonmissingValue.resize(y)
  } yield (t, v)

  @Test def testRandom() {
    Prop.forAll(genStructTypeAndNonMissingValue.filter(_._2 != null)) { case (t, a) =>
      println(t.toPrettyString(compact = true))
      println(a)
        val urb = new UnsafeRowBuilder(t)
        urb.convert(a.asInstanceOf[Row]) == a
    }.check()
  }
}
