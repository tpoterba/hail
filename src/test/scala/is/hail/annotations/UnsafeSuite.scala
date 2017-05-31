package is.hail.annotations

import is.hail.SparkSuite
import is.hail.check._
import is.hail.check.Arbitrary._
import is.hail.expr._
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class UnsafeSuite extends SparkSuite {

  val g = for {
    t <- Type.genStruct
    a <- t.genNonmissingValue} yield (t, a)

  @Test def testRandom() {
    Prop.forAll(g) { case (t, a) =>
        val urb = new UnsafeRowBuilder(t)
        urb.convert(a.asInstanceOf[Row]) == a
    }.check()
  }
}
