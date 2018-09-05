package is.hail.expr.types

import is.hail.utils._

abstract class TContainer extends Type {
  def elementType: Type

  override def children = FastSeq(elementType)
}
