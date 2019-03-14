package is.hail.expr.ir

object Binds {
  def apply(x: IR, v: String, i: Int): Boolean = {
    x match {
      case Let(n, _, _) =>
        v == n && i == 1
      case AggLet(n, _, _) =>
        v == n && i == 1
      case ArrayMap(_, n, _) =>
        v == n && i == 1
      case ArrayFlatMap(_, n, _) =>
        v == n && i == 1
      case ArrayFilter(_, n, _) =>
        v == n && i == 1
      case ArrayFold(_, _, accumName, valueName, _) =>
        (v == accumName || v == valueName) && i == 2
      case ArrayScan(_, _, accumName, valueName, _) =>
        (v == accumName || v == valueName) && i == 2
      case ArrayLeftJoinDistinct(_, _, l, r, _, _) =>
        (v == l || v == r) && i == 2
      case ArraySort(_, l, r, _) =>
        (v == l || v == r) && i == 2
      case AggExplode(_, n, _) =>
        v == n && i == 1
      case AggArrayPerElement(_, n, _) =>
        v == n && i == 1
      case ArrayAgg(_, name, _) =>
        v == name && i == 2
      case CollectDistributedArray(_, _, n1, n2, _) =>
        (v == n1 || v == n2) && i == 2
      case _ =>
        false
    }
  }
}
