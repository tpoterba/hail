package is.hail.expr.types.physical

import is.hail.annotations._
import is.hail.asm4s._
import is.hail.check.{Arbitrary, Gen}
import is.hail.expr.ir.{EmitMethodBuilder, IRParser, SortOrder}
import is.hail.expr.types.virtual._
import is.hail.expr.types.{BaseType, Requiredness}
import is.hail.expr.ir.{Ascending, Descending}
import is.hail.utils._
import is.hail.variant.ReferenceGenome
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JString

class PTypeSerializer extends CustomSerializer[PType](format => (
  { case JString(s) => PType.canonical(IRParser.parsePType(s)) },
  { case t: PType => JString(t.toString) }))


object PType {
  def genScalar(required: Boolean): Gen[PType] =
    Gen.oneOf(PBoolean(required), PInt32(required), PInt64(required), PFloat32(required),
      PFloat64(required), PString(required), PCall(required))

  val genOptionalScalar: Gen[PType] = genScalar(false)

  val genRequiredScalar: Gen[PType] = genScalar(true)

  def genComplexType(required: Boolean): Gen[ComplexPType] = {
    val rgDependents = ReferenceGenome.references.values.toArray.map(rg =>
      PLocus(rg, required))
    val others = Array(PCall(required))
    Gen.oneOfSeq(rgDependents ++ others)
  }

  def genFields(required: Boolean, genFieldType: Gen[PType]): Gen[Array[PField]] = {
    Gen.buildableOf[Array](
      Gen.zip(Gen.identifier, genFieldType))
      .filter(fields => fields.map(_._1).areDistinct())
      .map(fields => fields
        .iterator
        .zipWithIndex
        .map { case ((k, t), i) => PField(k, t, i) }
        .toArray)
  }

  def preGenStruct(required: Boolean, genFieldType: Gen[PType]): Gen[PStruct] = {
    for (fields <- genFields(required, genFieldType)) yield
      PStruct(fields, required)
  }

  def preGenTuple(required: Boolean, genFieldType: Gen[PType]): Gen[PTuple] = {
    for (fields <- genFields(required, genFieldType)) yield
      PTuple(required, fields.map(_.typ): _*)
  }

  private val defaultRequiredGenRatio = 0.2

  def genStruct: Gen[PStruct] = Gen.coin(defaultRequiredGenRatio).flatMap(preGenStruct(_, genArb))

  val genOptionalStruct: Gen[PType] = preGenStruct(required = false, genArb)

  val genRequiredStruct: Gen[PType] = preGenStruct(required = true, genArb)

  val genInsertableStruct: Gen[PStruct] = Gen.coin(defaultRequiredGenRatio).flatMap(required =>
    if (required)
      preGenStruct(required = true, genArb)
    else
      preGenStruct(required = false, genOptional))

  def genSized(size: Int, required: Boolean, genPStruct: Gen[PStruct]): Gen[PType] =
    if (size < 1)
      Gen.const(PStruct.empty(required))
    else if (size < 2)
      genScalar(required)
    else {
      Gen.frequency(
        (4, genScalar(required)),
        (1, genComplexType(required)),
        (1, genArb.map {
          PArray(_)
        }),
        (1, genArb.map {
          PSet(_)
        }),
        (1, genArb.map {
          PInterval(_)
        }),
        (1, preGenTuple(required, genArb)),
        (1, Gen.zip(genRequired, genArb).map { case (k, v) => PDict(k, v) }),
        (1, genPStruct.resize(size)))
    }

  def preGenArb(required: Boolean, genStruct: Gen[PStruct] = genStruct): Gen[PType] =
    Gen.sized(genSized(_, required, genStruct))

  def genArb: Gen[PType] = Gen.coin(0.2).flatMap(preGenArb(_))

  val genOptional: Gen[PType] = preGenArb(required = false)

  val genRequired: Gen[PType] = preGenArb(required = true)

  val genInsertable: Gen[PStruct] = genInsertableStruct

  implicit def arbType = Arbitrary(genArb)

  def canonical(t: Type, required: Boolean): PType = {
    t match {
      case _: TInt32 => PInt32(required)
      case _: TInt64 => PInt64(required)
      case _: TFloat32 => PFloat32(required)
      case _: TFloat64 => PFloat64(required)
      case _: TBoolean => PBoolean(required)
      case _: TBinary => PBinary(required)
      case _: TString => PString(required)
      case _: TCall => PCall(required)
      case t: TLocus => PLocus(t.rg, required)
      case t: TInterval => PInterval(canonical(t.pointType), required)
      case t: TStream => PStream(canonical(t.elementType), required)
      case t: TArray => PArray(canonical(t.elementType), required)
      case t: TSet => PSet(canonical(t.elementType), required)
      case t: TDict => PDict(canonical(t.keyType), canonical(t.valueType), required)
      case t: TTuple => PTuple(t._types.map(tf => PTupleField(tf.index, canonical(tf.typ))), required)
      case t: TStruct => PStruct(t.fields.map(f => PField(f.name, canonical(f.typ), f.index)), required)
      case t: TNDArray => PNDArray(canonical(t.elementType.setRequired(true)), t.nDims, required)
      case TVoid => PVoid
    }
  }

  def canonical(t: Type): PType = canonical(t, t.required)

  // currently identity
  def canonical(t: PType): PType = {
    t match {
      case t: PInt32 => PInt32(t.required)
      case t: PInt64 => PInt64(t.required)
      case t: PFloat32 => PFloat32(t.required)
      case t: PFloat64 => PFloat64(t.required)
      case t: PBoolean => PBoolean(t.required)
      case t: PBinary => PBinary(t.required)
      case t: PString => PString(t.required)
      case t: PCall => PCall(t.required)
      case t: PLocus => PLocus(t.rg, t.required)
      case t: PInterval => PInterval(canonical(t.pointType), t.required)
      case t: PStream => PStream(canonical(t.elementType), t.required)
      case t: PArray => PArray(canonical(t.elementType), t.required)
      case t: PSet => PSet(canonical(t.elementType), t.required)
      case t: PTuple => PTuple(t._types.map(pf => PTupleField(pf.index, canonical(pf.typ))), t.required)
      case t: PStruct => PStruct(t.fields.map(f => PField(f.name, canonical(f.typ), f.index)), t.required)
      case t: PNDArray => PNDArray(canonical(t.elementType), t.nDims, t.required)
      case t: PDict => PDict(canonical(t.keyType), canonical(t.valueType), t.required)
      case PVoid => PVoid
    }
  }
}

abstract class PType extends Serializable with Requiredness {
  self =>

  def virtualType: Type

  override def toString: String = {
    val sb = new StringBuilder
    pretty(sb, 0, true)
    sb.result()
  }

  def unsafeOrdering(): UnsafeOrdering = ???

  def isCanonical: Boolean = PType.canonical(this) == this // will recons, may need to rewrite this method

  def unsafeOrdering(rightType: PType): UnsafeOrdering = {
    require(this == rightType)
    unsafeOrdering()
  }

  def asIdent: String = (if (required) "r_" else "o_") + _asIdent

  def _asIdent: String

  final def pretty(sb: StringBuilder, indent: Int, compact: Boolean) {
    if (required)
      sb.append("+")
    _pretty(sb, indent, compact)
  }

  def _pretty(sb: StringBuilder, indent: Int, compact: Boolean)

  def codeOrdering(mb: EmitMethodBuilder): CodeOrdering =
    codeOrdering(mb, this)

  def codeOrdering(mb: EmitMethodBuilder, so: SortOrder): CodeOrdering =
    codeOrdering(mb, this, so)

  def codeOrdering(mb: EmitMethodBuilder, other: PType, so: SortOrder): CodeOrdering =
    so match {
      case Ascending => codeOrdering(mb, other)
      case Descending => codeOrdering(mb, other).reverse
    }

  def codeOrdering(mb: EmitMethodBuilder, other: PType): CodeOrdering

  def byteSize: Long = 1

  def alignment: Long = byteSize

  /*  Fundamental types are types that can be handled natively by RegionValueBuilder: primitive
      types, Array and Struct. */
  def fundamentalType: PType = this

  final def unary_+(): PType = setRequired(true)

  final def unary_-(): PType = setRequired(false)

  final def setRequired(required: Boolean): PType = {
    if (required == this.required)
      this
    else
      this match {
        case PBinary(_) => PBinary(required)
        case PBoolean(_) => PBoolean(required)
        case PInt32(_) => PInt32(required)
        case PInt64(_) => PInt64(required)
        case PFloat32(_) => PFloat32(required)
        case PFloat64(_) => PFloat64(required)
        case PString(_) => PString(required)
        case t: PCall => t.copy(required)
        case t: PArray => t.copy(required = required)
        case t: PSet => t.copy(required = required)
        case t: PDict => t.copy(required = required)
        case t: PLocus => t.copy(required = required)
        case t: PInterval => t.copy(required = required)
        case t: PStruct => t.copy(required = required)
        case t: PTuple => t.copy(required = required)
      }
  }

  final def isOfType(t: PType): Boolean = {
    this match {
      case PBinary(_) => t == PCanonicalBinaryOptional || t == PCanonicalBinaryRequired
      case PBoolean(_) => t == PBooleanOptional || t == PBooleanRequired
      case PInt32(_) => t == PInt32Optional || t == PInt32Required
      case PInt64(_) => t == PInt64Optional || t == PInt64Required
      case PFloat32(_) => t == PFloat32Optional || t == PFloat32Required
      case PFloat64(_) => t == PFloat64Optional || t == PFloat64Required
      case _: PString => t.isInstanceOf[PString]
      case _: PCall => t.isInstanceOf[PCall]
      case t2: PLocus => t.isInstanceOf[PLocus] && t.asInstanceOf[PLocus].rg == t2.rg
      case t2: PInterval => t.isInstanceOf[PInterval] && t.asInstanceOf[PInterval].pointType.isOfType(t2.pointType)
      case t2: PStruct =>
        t.isInstanceOf[PStruct] &&
          t.asInstanceOf[PStruct].size == t2.size &&
          t.asInstanceOf[PStruct].fields.zip(t2.fields).forall { case (f1: PField, f2: PField) => f1.typ.isOfType(f2.typ) && f1.name == f2.name }
      case t2: PTuple =>
        t.isInstanceOf[PTuple] &&
          t.asInstanceOf[PTuple].size == t2.size &&
          t.asInstanceOf[PTuple].types.zip(t2.types).forall { case (typ1, typ2) => typ1.isOfType(typ2) }
      case t2: PArray => t.isInstanceOf[PArray] && t.asInstanceOf[PArray].elementType.isOfType(t2.elementType)
      case t2: PSet => t.isInstanceOf[PSet] && t.asInstanceOf[PSet].elementType.isOfType(t2.elementType)
      case t2: PDict => t.isInstanceOf[PDict] && t.asInstanceOf[PDict].keyType.isOfType(t2.keyType) && t.asInstanceOf[PDict].valueType.isOfType(t2.valueType)
    }
  }

  final def isPrimitive: Boolean =
    fundamentalType.isInstanceOf[PBoolean] || isNumeric

  final def isNumeric: Boolean =
    fundamentalType.isInstanceOf[PInt32] ||
      fundamentalType.isInstanceOf[PInt64] ||
      fundamentalType.isInstanceOf[PFloat32] ||
      fundamentalType.isInstanceOf[PFloat64]

  def containsPointers: Boolean = false

  def subsetTo(t: Type): PType = {
    this match {
      case PCanonicalStruct(fields, r) =>
        val ts = t.asInstanceOf[TStruct]
        PCanonicalStruct(r, fields.flatMap { pf => ts.fieldOption(pf.name).map { vf => (pf.name, pf.typ.subsetTo(vf.typ)) } }: _*)
      case PCanonicalTuple(fields, r) =>
        val tt = t.asInstanceOf[TTuple]
        PCanonicalTuple(fields.flatMap { pf => tt.fieldIndex.get(pf.index).map(vi => PTupleField(vi, pf.typ.subsetTo(tt.types(vi)))) }, r)
      case PCanonicalArray(e, r) =>
        val ta = t.asInstanceOf[TArray]
        PCanonicalArray(e.subsetTo(ta.elementType), r)
      case PCanonicalSet(e, r) =>
        val ts = t.asInstanceOf[TSet]
        PCanonicalSet(e.subsetTo(ts.elementType), r)
      case PCanonicalDict(k, v, r) =>
        val td = t.asInstanceOf[TDict]
        PCanonicalDict(k.subsetTo(td.keyType), v.subsetTo(td.valueType), r)
      case PCanonicalInterval(p, r) =>
        val ti = t.asInstanceOf[TInterval]
        PCanonicalInterval(p.subsetTo(ti.pointType), r)
      case _ =>
        assert(virtualType == t)
        this
    }
  }

  def deepInnerRequired(required: Boolean): PType =
    this match {
      case t: PArray => PArray(t.elementType.deepInnerRequired(true), required)
      case t: PSet => PSet(t.elementType.deepInnerRequired(true), required)
      case t: PDict => PDict(t.keyType.deepInnerRequired(true), t.valueType.deepInnerRequired(true), required)
      case t: PStruct =>
        PStruct(t.fields.map(f => PField(f.name, f.typ.deepInnerRequired(true), f.index)), required)
      case t: PTuple =>
        PTuple(required, t.types.map(_.deepInnerRequired(true)): _*)
      case t: PInterval =>
        PInterval(t.pointType.deepInnerRequired(true), required)
      case t =>
        t.setRequired(required)
    }

  // Semantics: must be callable without requiredeness check: srcAddress must point to non-null value
  def copyFromType(mb: MethodBuilder, region: Code[Region], srcPType: PType, srcAddress: Code[Long], forceDeep: Boolean): Code[Long]

  def copyFromTypeAndStackValue(mb: MethodBuilder, region: Code[Region], srcPType: PType, stackValue: Code[_], forceDeep: Boolean): Code[_]

  def copyFromType(mb: MethodBuilder, region: Code[Region], srcPType: PType, srcAddress: Code[Long]): Code[Long] =
    this.copyFromType(mb, region, srcPType, srcAddress, false)

  def copyFromTypeAndStackValue(mb: MethodBuilder, region: Code[Region], srcPType: PType, stackValue: Code[_]): Code[_] =
    this.copyFromTypeAndStackValue(mb, region, srcPType, stackValue, false)

  def copyFromType(region: Region, srcPType: PType, srcAddress: Long, forceDeep: Boolean): Long

  def copyFromType(region: Region, srcPType: PType, srcAddress: Long): Long =
    this.copyFromType(region, srcPType, srcAddress, false)

  def deepRename(t: Type) = this

  def storeShallowAtOffset(dstAddress: Code[Long], srcAddress: Code[Long]): Code[Unit]

  def storeShallowAtOffset(dstAddress: Long, srcAddress: Long)
}
