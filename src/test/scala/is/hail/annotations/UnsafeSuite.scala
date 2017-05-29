package is.hail.annotations

import is.hail.SparkSuite
import is.hail.check._
import is.hail.check.Arbitrary._
import is.hail.expr._
import is.hail.keytable.KeyTable
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

//  val supp: Array[Type] = Array(TInt, TString, TDict(TString, TInt),TDouble, TLong, TFloat, TBoolean, TArray(TInt), TArray(TBoolean), TArray(TArray(TInt)))
  val supp: Array[Type] = Array(TInt, TVariant, TGenotype, TStruct("foo" -> TInt, "bar" -> TString), TArray( TStruct("foo" -> TInt, "bar" -> TString)), TString, TDict(TString, TInt),TDouble, TLong, TFloat, TBoolean, TArray(TInt), TArray(TBoolean), TArray(TArray(TInt)))

  def fieldGen = Gen.zip(arbitrary[String], Gen.choose(0, supp.length - 1).map(supp))

  def structGen = for {
    n <- Gen.choose(1, 100)
    struct <- Gen.buildableOfN[Array, (String, Type)](n, fieldGen).map(fs => TStruct(fs: _*))
    value <- struct.genNonmissingValue.map(_.asInstanceOf[Row])
  } yield (struct, value)


  @Test def testRandom() {

//    Prop.forAll(structGen.resize(100000)) { case (t, a) =>
//
////      println(s"t = ${t.toPrettyString(compact=true)}")
////      println(s"a = $a")
//      val urb = new UnsafeRowBuilder(t, debug = false)
//
//      urb.ingest(a)
//      val res = urb.result()
////      println((0 until a.length).map(i => i -> a.isNullAt(i)))
////      println((0 until a.length).map(i => i -> res.isNullAt(i)))
//      val newRow = res
//      val p = newRow == a
//      if (!p) {
//        println(
//          s"""Error! mismatch:
//             |  old: $a
//             |  new: $newRow""".stripMargin)
//      }
//      newRow == a
//    }.check()



    val g = for {t <- Type.genArb.filter(_.isInstanceOf[TStruct]).map(_.asInstanceOf[TStruct])
      a <- t.genNonmissingValue} yield (t, a)
    Prop.forAll(g) { case (t, a) =>
        val urb = new UnsafeRowBuilder(t, debug = false)
        urb.ingest(a.asInstanceOf[Row])
        urb.result() == a
    }.check()
  }

  @Test def testWrite() {
    val kt = hc.importTable("src/test/resources/variantAnnotations.tsv", impute=true)
    .repartition(1)
    .keyBy("Position")
      kt.writeRS("/tmp/test.rs", overwrite = true)

    val rb = KeyTable.readRS(hc, "/tmp/test.rs")
    println(rb.count())

    assert(kt.same(rb))
  }
}
