package is.hail.annotations

import is.hail.SparkSuite
import is.hail.check._
import is.hail.expr._
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class UnsafeSuite extends SparkSuite {

  @Test def testMemoryBuffer() {
    val buff = new MemoryBuffer()

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

  val g = (for {
    s <- Gen.size
    // prefer smaller type and bigger values
    fraction <- Gen.choose(0.1, 0.3)
    x = (fraction * s).toInt
    y = s - x
    t <- Type.genStruct.resize(x)
    v <- t.genNonmissingValue.resize(y)
  } yield (t, v)).filter(_._2 != null)

  @Test def testCreation() {

    val rng = new RandomDataGenerator()
    rng.reSeed(Prop.seed)

    Prop.forAll(g) { case (t, a) =>
      val urb = new UnsafeRowBuilder(t, debug = false)
      urb.setAll(a.asInstanceOf[Row])
      val unsafeRow = urb.result()

      urb.clear()
      urb.setAll(a.asInstanceOf[Row])
      val ur2 = urb.result()
      val p = unsafeRow == a

      assert(unsafeRow == ur2)

      if (!p) {
        println(
          s"""IN:  $a
             |OUT: $unsafeRow""".stripMargin)
      }

      p
    }.apply(Parameters(rng, 1000, 1000))
  }

  @Test def testSubset() {

    val rng = new RandomDataGenerator()
    rng.reSeed(Prop.seed)

    val g2 = for {
      x <- g
      r <- Gen.parameterized { p => Gen.const((0 until x._1.size).toArray).filter(_ => p.rng.nextInt(0, 100) < 50) }
    } yield (x, r)

    Prop.forAll(g2) { case ((t, a), r) =>
      val urb = new UnsafeRowBuilder(t, debug = false)
      val row = a.asInstanceOf[Row]
      var i = 0
      urb.setAll(row)
      val ur1 = urb.result()
      urb.clear()
      val p1 = ur1 == row

      val t2 = TStruct(r.map(t.fields).map(f => f.name -> f.typ): _*)
      val urb2 = new UnsafeRowBuilder(t2, debug = false)

      i = 0

      while (i < t2.size) {
        urb2.setFromUnsafe(i, r(i), ur1)
        i += 1
      }
      val ur2 = urb2.result()
      urb2.clear()

      i = 0
      while (i < t2.size) {
        urb2.setFromRow(i, r(i), ur1)
        i += 1
      }
      val ur3 = urb2.result()
      val p2 = ur3 == ur2

      val p3 = ur2 == Row.fromSeq(r.map(row.get))

      val p = p1 && p2 && p3

      if (!p) {
        println(
          s"""SCHEMA: $t
             |IN:   $a
             |OUT1: $ur1
             |OUT2: $ur2
             |SIZE: ${ ur1.ptr.mb.sizeInBytes }/${ ur2.ptr.mb.sizeInBytes }""".stripMargin)
      }

      p
    }.apply(Parameters(rng, 1000, 1000))
  }
}