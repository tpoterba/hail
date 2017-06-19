package is.hail.annotations

import is.hail.SparkSuite
import is.hail.check._
import is.hail.check.Arbitrary._
import is.hail.expr._
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class UnsafeSuite extends SparkSuite {

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
