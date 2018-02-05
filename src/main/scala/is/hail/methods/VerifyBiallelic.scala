package is.hail.methods

import is.hail.annotations.UnsafeRow
import is.hail.variant.{MatrixTable, Variant}
import is.hail.utils._

object VerifyBiallelic {
  def apply(vsm: MatrixTable, method: String): MatrixTable = {
    val localRowType = vsm.rvRowType
    vsm.copy2(
      rvd = vsm.rvd.mapPreservesPartitioning(vsm.rvd.typ) { rv =>
        val ur = new UnsafeRow(localRowType, rv.region, rv.offset)
        val v = ur.getAs[Variant](1)
        if (!v.isBiallelic)
          fatal(s"in $method: found non-biallelic variant: $v")
        rv
      })
  }
}
