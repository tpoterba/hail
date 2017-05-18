package is.hail.annotations

import is.hail.SparkSuite
import is.hail.expr._
import org.testng.annotations.Test
import sun.misc.Unsafe

class UnsafeSuite extends SparkSuite {
  @Test def test() {
    val u = Unsafe.getUnsafe

    val l = u.allocateMemory(100)

    TInt.writeUnsafe(u, l, 5)
    println(TInt.readUnsafe(u, l))

    u.freeMemory(l)
  }
}
