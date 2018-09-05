package is.hail.expr.types.physical

import is.hail.expr.types._

object CanonicalPType {

  // This method inverts Type requiredness, lifting it up as properties of array elements or struct fields
  // When requiredness on virtual types is removed, this will get simpler.
  private def fromFundamental(t: Type, virtualType: Type): PType = (t, virtualType) match {
    case (_: TInt32, _) => PInt32
    case (_: TInt64, _) => PInt64
    case (_: TFloat32, _) => PFloat32
    case (_: TFloat64, _) => PFloat64
    case (_: TBoolean, _) => PBool
    case (TVoid, _) => PVoid
    case (_: TBinary | _: TString, _) => PBinary(virtualType)
    case (tc: TContainer, vt: TContainer) =>
      PCanonicalArray(fromFundamental(tc.elementType, vt.elementType), vt, tc.elementType.required)
    case (ts: TStruct, vs: TStruct) => PCanonicalStruct(
      ts.fields
        .zip(vs.fields)
        .map { case (fund, virt) => (fund.name, fund.typ.required, fromFundamental(fund.typ, virt.typ))},
      vs
    )
  }

  def apply(t: Type): PType = fromFundamental(t.fundamentalType, t)
}
