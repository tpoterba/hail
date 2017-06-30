package is.hail.annotations

import is.hail.SparkSuite
import is.hail.check._
import is.hail.expr._
import org.apache.commons.math3.random.RandomDataGenerator
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

  val g = for {
    s <- Gen.size
    // prefer smaller type and bigger values
    fraction <- Gen.choose(0.1, 0.3)
    x = (fraction * s).toInt
    y = s - x
    t <- Type.genStruct.resize(x)
    v <- t.genNonmissingValue.resize(y)
  } yield (t, v)

  @Test def testRandom() {

    val rng = new RandomDataGenerator()
    rng.reSeed(Prop.seed)

    Prop.forAll(g.filter(_._2 != null)) { case (t, a) =>
      val urb = new UnsafeRowBuilder(t, debug = false)
      val unsafeRow = urb.convert(a.asInstanceOf[Row])
      val p = unsafeRow == a
      if (!p) {
        println(
          s"""IN:  $a
             |OUT: $unsafeRow""".stripMargin)
      }

      p
    }.apply(Parameters(rng, 1000, 1000))
  }
}
