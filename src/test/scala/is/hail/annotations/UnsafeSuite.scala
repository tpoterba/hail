package is.hail.annotations

import is.hail.SparkSuite
import is.hail.expr._
import org.testng.annotations.Test
import sun.misc.Unsafe

class UnsafeSuite extends SparkSuite {
  @Test def test() {

    val urb = new UnsafeRowBuilder(TStruct("foo" -> TInt, "bar" -> TInt))
    urb.putInt(4)
    urb.putInt(4)
    val r = urb.result()
    println(r.getInt(0))
    println(r.getInt(1))
  }
}
