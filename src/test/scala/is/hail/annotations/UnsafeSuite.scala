package is.hail.annotations

import is.hail.SparkSuite
import is.hail.check._
import is.hail.check.Arbitrary._
import is.hail.expr._
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class UnsafeSuite extends SparkSuite {

  @Test def testRandom() {

    val g = for {t <- Type.genArb.filter(_.isInstanceOf[TStruct]).map(_.asInstanceOf[TStruct])
      a <- t.genNonmissingValue} yield (t, a)

    Prop.forAll(g) { case (t, a) =>
        val urb = new UnsafeRowBuilder(t)
        urb.convert(a.asInstanceOf[Row]) == a
    }.check()
  }
}
