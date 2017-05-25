package is.hail.annotations

import is.hail.SparkSuite
import is.hail.check._
import is.hail.check.Arbitrary._
import is.hail.expr._
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class UnsafeSuite extends SparkSuite {
//  @Test def test() {
//
//    val urb = new UnsafeRowBuilder(TStruct("foo" -> TInt, "bar" -> TInt))
//    urb.putInt(4)
//    urb.putInt(4)
//    val r = urb.result()
//    println(r.get(0))
//    println(r.get(1))
//
//  }

  val supp: Array[Type] = Array(TInt, TDouble, TLong, TFloat, TBoolean, TArray(TInt), TArray(TBoolean), TArray(TArray(TInt)))

  def fieldGen = Gen.zip(arbitrary[String], Gen.choose(0, supp.length - 1).map(supp))

  def structGen = for {
    n <- Gen.choose(1, 100)
    struct <- Gen.buildableOfN[Array, (String, Type)](n, fieldGen).map(fs => TStruct(fs: _*))
    value <- struct.genNonmissingValue.map(_.asInstanceOf[Row])
  } yield (struct, value)


  @Test def testRandom() {

    Prop.forAll(structGen.resize(100000)) { case (t, a) =>

//      println(s"t = ${t.toPrettyString(compact=true)}")
//      println(s"a = $a")
      val urb = new UnsafeRowBuilder(t)

      urb.ingest(a)
      val res = urb.result()
//      println((0 until a.length).map(i => i -> a.isNullAt(i)))
//      println((0 until a.length).map(i => i -> res.isNullAt(i)))
      val newRow = res
//      println(a)
//      println(newRow)
      newRow == a
    }.check()
  }
}
