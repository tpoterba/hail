package org.broadinstitute.hail.expr

import org.apache.spark.sql.Row
import org.apache.spark.util.StatCounter
import org.broadinstitute.hail.FatalException
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.stats._
import org.broadinstitute.hail.utils.Interval
import org.broadinstitute.hail.variant.{AltAllele, Genotype, Locus, Variant}
import org.json4s.jackson.JsonMethods

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.input.{Position, Positional}
import scala.language.existentials

case class EvalContext(st: SymbolTable, a: ArrayBuffer[Any], aggregationFunctions: ArrayBuffer[Aggregator]) {

  def setAll(args: Any*) {
    args.zipWithIndex.foreach { case (arg, i) => a(i) = arg }
  }

  def set(index: Int, arg: Any) {
    a(index) = arg
  }
}

object EvalContext {
  def apply(symTab: SymbolTable): EvalContext = {
    val a = new ArrayBuffer[Any]()
    val af = new ArrayBuffer[Aggregator]()
    for ((i, t) <- symTab.values) {
      if (i >= 0)
        a += null
    }

    EvalContext(symTab, a, af)
  }

  def apply(args: (String, Type)*): EvalContext = {
    val st = args.zipWithIndex
      .map { case ((name, t), i) => (name, (i, t)) }
      .toMap
    EvalContext(st)
  }
}

trait NumericConversion[T] extends Serializable {
  def to(numeric: Any): T
}

object IntNumericConversion extends NumericConversion[Int] {
  def to(numeric: Any): Int = numeric match {
    case i: Int => i
  }
}

object LongNumericConversion extends NumericConversion[Long] {
  def to(numeric: Any): Long = numeric match {
    case i: Int => i
    case l: Long => l
  }
}

object FloatNumericConversion extends NumericConversion[Float] {
  def to(numeric: Any): Float = numeric match {
    case i: Int => i
    case l: Long => l
    case f: Float => f
  }
}

object DoubleNumericConversion extends NumericConversion[Double] {
  def to(numeric: Any): Double = numeric match {
    case i: Int => i
    case l: Long => l
    case f: Float => f
    case d: Double => d
  }
}

object AST extends Positional {
  def promoteNumeric(t: TNumeric): BaseType = t

  def promoteNumeric(lhs: TNumeric, rhs: TNumeric): TNumeric =
    if (lhs == TDouble || rhs == TDouble)
      TDouble
    else if (lhs == TFloat || rhs == TFloat)
      TFloat
    else if (lhs == TLong || rhs == TLong)
      TLong
    else
      TInt

  def evalFlatCompose[T](ec: EvalContext, subexpr: AST)
    (g: (T) => Option[Any]): () => Any = {
    val f = subexpr.eval(ec)
    () => {
      val x = f()
      if (x != null)
        g(x.asInstanceOf[T]).orNull
      else
        null
    }
  }

  def evalCompose[T](ec: EvalContext, subexpr: AST)
    (g: (T) => Any): () => Any = {
    val f = subexpr.eval(ec)
    () => {
      val x = f()
      if (x != null)
        g(x.asInstanceOf[T])
      else
        null
    }
  }

  def evalCompose[T1, T2](ec: EvalContext, subexpr1: AST, subexpr2: AST)
    (g: (T1, T2) => Any): () => Any = {
    val f1 = subexpr1.eval(ec)
    val f2 = subexpr2.eval(ec)
    () => {
      val x = f1()
      if (x != null) {
        val y = f2()
        if (y != null)
          g(x.asInstanceOf[T1], y.asInstanceOf[T2])
        else
          null
      } else
        null
    }
  }

  def evalCompose[T1, T2, T3](ec: EvalContext, subexpr1: AST, subexpr2: AST, subexpr3: AST)
    (g: (T1, T2, T3) => Any): () => Any = {
    val f1 = subexpr1.eval(ec)
    val f2 = subexpr2.eval(ec)
    val f3 = subexpr3.eval(ec)
    () => {
      val x = f1()
      if (x != null) {
        val y = f2()
        if (y != null) {
          val z = f3()
          if (z != null)
            g(x.asInstanceOf[T1], y.asInstanceOf[T2], z.asInstanceOf[T3])
          else
            null
        } else
          null
      } else
        null
    }
  }

  def evalCompose[T1, T2, T3, T4](ec: EvalContext, subexpr1: AST, subexpr2: AST, subexpr3: AST, subexpr4: AST)
    (g: (T1, T2, T3, T4) => Any): () => Any = {
    val f1 = subexpr1.eval(ec)
    val f2 = subexpr2.eval(ec)
    val f3 = subexpr3.eval(ec)
    val f4 = subexpr4.eval(ec)
    () => {
      val w = f1()
      if (w != null) {
        val x = f2()
        if (x != null) {
          val y = f3()
          if (y != null) {
            val z = f4()
            if (z != null)
              g(w.asInstanceOf[T1], x.asInstanceOf[T2], y.asInstanceOf[T3], z.asInstanceOf[T4])
            else
              null
          } else
            null
        } else
          null
      } else
        null
    }
  }

  def evalComposeNumeric[T](ec: EvalContext, subexpr: AST)
    (g: (T) => Any)
    (implicit convT: NumericConversion[T]): () => Any = {
    val f = subexpr.eval(ec)
    () => {
      val x = f()
      if (x != null)
        g(convT.to(x))
      else
        null
    }
  }


  def evalComposeNumeric[T1, T2](ec: EvalContext, subexpr1: AST, subexpr2: AST)
    (g: (T1, T2) => Any)
    (implicit convT1: NumericConversion[T1], convT2: NumericConversion[T2]): () => Any = {
    val f1 = subexpr1.eval(ec)
    val f2 = subexpr2.eval(ec)
    () => {
      val x = f1()
      if (x != null) {
        val y = f2()
        if (y != null)
          g(convT1.to(x), convT2.to(y))
        else
          null
      } else
        null
    }
  }
}

case class Positioned[T](x: T) extends Positional

sealed abstract class AST(pos: Position, subexprs: Array[AST] = Array.empty) {
  var `type`: BaseType = null

  def this(posn: Position, subexpr1: AST) = this(posn, Array(subexpr1))

  def this(posn: Position, subexpr1: AST, subexpr2: AST) = this(posn, Array(subexpr1, subexpr2))

  def eval(ec: EvalContext): () => Any

  def typecheckThis(ec: EvalContext): BaseType = typecheckThis()

  def typecheckThis(): BaseType = throw new UnsupportedOperationException

  def typecheck(ec: EvalContext) {
    subexprs.foreach(_.typecheck(ec))
    `type` = typecheckThis(ec)
  }

  def parseError(msg: String): Nothing = ParserUtils.error(pos, msg)
}

case class Const(posn: Position, value: Any, t: BaseType) extends AST(posn) {
  def eval(c: EvalContext): () => Any = {
    val v = value
    () => v
  }

  override def typecheckThis(): BaseType = t
}

case class Select(posn: Position, lhs: AST, rhs: String) extends AST(posn, lhs) {
  override def typecheckThis(): BaseType = {
    (lhs.`type`, rhs) match {
      case (TSample, "id") => TString
      case (TGenotype, "gt") => TInt
      case (TGenotype, "gtj") => TInt
      case (TGenotype, "gtk") => TInt
      case (TGenotype, "ad") => TArray(TInt)
      case (TGenotype, "dp") => TInt
      case (TGenotype, "od") => TInt
      case (TGenotype, "gq") => TInt
      case (TGenotype, "pl") => TArray(TInt)
      case (TGenotype, "dosage") => TArray(TDouble)
      case (TGenotype, "isHomRef") => TBoolean
      case (TGenotype, "isHet") => TBoolean
      case (TGenotype, "isHomVar") => TBoolean
      case (TGenotype, "isCalledNonRef") => TBoolean
      case (TGenotype, "isHetNonRef") => TBoolean
      case (TGenotype, "isHetRef") => TBoolean
      case (TGenotype, "isCalled") => TBoolean
      case (TGenotype, "isNotCalled") => TBoolean
      case (TGenotype, "nNonRefAlleles") => TInt
      case (TGenotype, "pAB") => TDouble
      case (TGenotype, "fractionReadsRef") => TDouble
      case (TGenotype, "fakeRef") => TBoolean
      case (TGenotype, "isDosage") => TBoolean

      case (TVariant, "contig") => TString
      case (TVariant, "start") => TInt
      case (TVariant, "ref") => TString
      case (TVariant, "altAlleles") => TArray(TAltAllele)
      case (TVariant, "nAltAlleles") => TInt
      case (TVariant, "nAlleles") => TInt
      case (TVariant, "isBiallelic") => TBoolean
      case (TVariant, "nGenotypes") => TInt
      case (TVariant, "inXPar") => TBoolean
      case (TVariant, "inYPar") => TBoolean
      case (TVariant, "inXNonPar") => TBoolean
      case (TVariant, "inYNonPar") => TBoolean
      case (TVariant, "locus") => TLocus

      // assumes biallelic
      case (TVariant, "alt") => TString
      case (TVariant, "altAllele") => TAltAllele

      case (TLocus, "contig") => TString
      case (TLocus, "position") => TInt

      case (TInterval, "start") => TLocus
      case (TInterval, "end") => TLocus

      case (TAltAllele, "ref") => TString
      case (TAltAllele, "alt") => TString
      case (TAltAllele, "isSNP") => TBoolean
      case (TAltAllele, "isMNP") => TBoolean
      case (TAltAllele, "isIndel") => TBoolean
      case (TAltAllele, "isInsertion") => TBoolean
      case (TAltAllele, "isDeletion") => TBoolean
      case (TAltAllele, "isComplex") => TBoolean
      case (TAltAllele, "isTransition") => TBoolean
      case (TAltAllele, "isTransversion") => TBoolean

      case (t: TStruct, _) =>
        t.selfField(rhs) match {
          case Some(f) => f.`type`
          case None => parseError(s"`$t' has no field `$rhs")
        }

      case (t: TNumeric, "toInt") => TInt
      case (t: TNumeric, "toLong") => TLong
      case (t: TNumeric, "toFloat") => TFloat
      case (t: TNumeric, "toDouble") => TDouble
      case (TString, "toInt") => TInt
      case (TString, "toLong") => TLong
      case (TString, "toFloat") => TFloat
      case (TString, "toDouble") => TDouble
      case (t: TNumeric, "abs") => t
      case (t: TNumeric, "signum") => TInt
      case (TString, "length") => TInt
      case (t: TArray, "length") => TInt
      case (t: TIterable, "size") => TInt
      case (t: TIterable, "isEmpty") => TBoolean
      case (t: TIterable, "toSet") => TSet(t.elementType)
      case (t: TIterable, "toArray") => TArray(t.elementType)
      case (t: TDict, "size") => TInt
      case (t: TDict, "isEmpty") => TBoolean
      case (TArray(elementType: TNumeric), "sum" | "min" | "max") => elementType
      case (TSet(elementType: TNumeric), "sum" | "min" | "max") => elementType
      case (TArray(elementType), "head") => elementType
      case (t@TArray(elementType), "tail") => t

      case (t, _) =>
        parseError(s"`$t' has no field `$rhs'")
    }
  }

  def eval(ec: EvalContext): () => Any = ((lhs.`type`, rhs): @unchecked) match {
    case (TSample, "id") => lhs.eval(ec)
    case (TGenotype, "gt") =>
      AST.evalFlatCompose[Genotype](ec, lhs)(_.gt)
    case (TGenotype, "gtj") =>
      AST.evalFlatCompose[Genotype](ec, lhs)(_.gt.map(gtx => Genotype.gtPair(gtx).j))
    case (TGenotype, "gtk") =>
      AST.evalFlatCompose[Genotype](ec, lhs)(_.gt.map(gtx => Genotype.gtPair(gtx).k))
    case (TGenotype, "ad") =>
      AST.evalFlatCompose[Genotype](ec, lhs)(g => g.ad.map(a => a: IndexedSeq[Int]))
    case (TGenotype, "dp") =>
      AST.evalFlatCompose[Genotype](ec, lhs)(_.dp)
    case (TGenotype, "od") =>
      AST.evalFlatCompose[Genotype](ec, lhs)(_.od)
    case (TGenotype, "gq") =>
      AST.evalFlatCompose[Genotype](ec, lhs)(_.gq)
    case (TGenotype, "pl") =>
      AST.evalFlatCompose[Genotype](ec, lhs)(g => g.pl.map(a => a: IndexedSeq[Int]))
    case (TGenotype, "dosage") =>
      AST.evalFlatCompose[Genotype](ec, lhs)(g => g.dosage.map(a => a: IndexedSeq[Double]))
    case (TGenotype, "isHomRef") =>
      AST.evalCompose[Genotype](ec, lhs)(_.isHomRef)
    case (TGenotype, "isHet") =>
      AST.evalCompose[Genotype](ec, lhs)(_.isHet)
    case (TGenotype, "isHomVar") =>
      AST.evalCompose[Genotype](ec, lhs)(_.isHomVar)
    case (TGenotype, "isCalledNonRef") =>
      AST.evalCompose[Genotype](ec, lhs)(_.isCalledNonRef)
    case (TGenotype, "isHetNonRef") =>
      AST.evalCompose[Genotype](ec, lhs)(_.isHetNonRef)
    case (TGenotype, "isHetRef") =>
      AST.evalCompose[Genotype](ec, lhs)(_.isHetRef)
    case (TGenotype, "isCalled") =>
      AST.evalCompose[Genotype](ec, lhs)(_.isCalled)
    case (TGenotype, "isNotCalled") =>
      AST.evalCompose[Genotype](ec, lhs)(_.isNotCalled)
    case (TGenotype, "nNonRefAlleles") => AST.evalFlatCompose[Genotype](ec, lhs)(_.nNonRefAlleles)
    case (TGenotype, "pAB") =>
      AST.evalFlatCompose[Genotype](ec, lhs)(_.pAB())
    case (TGenotype, "fractionReadsRef") =>
      AST.evalFlatCompose[Genotype](ec, lhs)(_.fractionReadsRef())
    case (TGenotype, "fakeRef") =>
      AST.evalCompose[Genotype](ec, lhs)(_.fakeRef)
    case (TGenotype, "isDosage") =>
      AST.evalCompose[Genotype](ec, lhs)(_.isDosage)

    case (TVariant, "contig") =>
      AST.evalCompose[Variant](ec, lhs)(_.contig)
    case (TVariant, "start") =>
      AST.evalCompose[Variant](ec, lhs)(_.start)
    case (TVariant, "ref") =>
      AST.evalCompose[Variant](ec, lhs)(_.ref)
    case (TVariant, "altAlleles") =>
      AST.evalCompose[Variant](ec, lhs)(_.altAlleles)
    case (TVariant, "nAltAlleles") =>
      AST.evalCompose[Variant](ec, lhs)(_.nAltAlleles)
    case (TVariant, "nAlleles") =>
      AST.evalCompose[Variant](ec, lhs)(_.nAlleles)
    case (TVariant, "isBiallelic") =>
      AST.evalCompose[Variant](ec, lhs)(_.isBiallelic)
    case (TVariant, "nGenotypes") =>
      AST.evalCompose[Variant](ec, lhs)(_.nGenotypes)
    case (TVariant, "inXPar") =>
      AST.evalCompose[Variant](ec, lhs)(_.inXPar)
    case (TVariant, "inYPar") =>
      AST.evalCompose[Variant](ec, lhs)(_.inYPar)
    case (TVariant, "inXNonPar") =>
      AST.evalCompose[Variant](ec, lhs)(_.inXNonPar)
    case (TVariant, "inYNonPar") =>
      AST.evalCompose[Variant](ec, lhs)(_.inYNonPar)
    // assumes biallelic
    case (TVariant, "alt") =>
      AST.evalCompose[Variant](ec, lhs)(_.alt)
    case (TVariant, "altAllele") =>
      AST.evalCompose[Variant](ec, lhs)(_.altAllele)
    case (TVariant, "locus") =>
      AST.evalCompose[Variant](ec, lhs)(_.locus)

    case (TLocus, "contig") =>
      AST.evalCompose[Locus](ec, lhs)(_.contig)
    case (TLocus, "position") =>
      AST.evalCompose[Locus](ec, lhs)(_.position)

    case (TInterval, "start") =>
      AST.evalCompose[Interval[Locus]](ec, lhs)(_.start)
    case (TInterval, "end") =>
      AST.evalCompose[Interval[Locus]](ec, lhs)(_.end)

    case (TAltAllele, "ref") => AST.evalCompose[AltAllele](ec, lhs)(_.ref)
    case (TAltAllele, "alt") => AST.evalCompose[AltAllele](ec, lhs)(_.alt)
    case (TAltAllele, "isSNP") => AST.evalCompose[AltAllele](ec, lhs)(_.isSNP)
    case (TAltAllele, "isMNP") => AST.evalCompose[AltAllele](ec, lhs)(_.isMNP)
    case (TAltAllele, "isIndel") => AST.evalCompose[AltAllele](ec, lhs)(_.isIndel)
    case (TAltAllele, "isInsertion") => AST.evalCompose[AltAllele](ec, lhs)(_.isInsertion)
    case (TAltAllele, "isDeletion") => AST.evalCompose[AltAllele](ec, lhs)(_.isDeletion)
    case (TAltAllele, "isComplex") => AST.evalCompose[AltAllele](ec, lhs)(_.isComplex)
    case (TAltAllele, "isTransition") => AST.evalCompose[AltAllele](ec, lhs)(_.isTransition)
    case (TAltAllele, "isTransversion") => AST.evalCompose[AltAllele](ec, lhs)(_.isTransversion)

    case (t: TStruct, _) =>
      val Some(f) = t.selfField(rhs)
      val i = f.index
      AST.evalCompose[Row](ec, lhs)(_.get(i))

    case (TInt, "toInt") => lhs.eval(ec)
    case (TInt, "toLong") => AST.evalCompose[Int](ec, lhs)(_.toLong)
    case (TInt, "toFloat") => AST.evalCompose[Int](ec, lhs)(_.toFloat)
    case (TInt, "toDouble") => AST.evalCompose[Int](ec, lhs)(_.toDouble)

    case (TLong, "toInt") => AST.evalCompose[Long](ec, lhs)(_.toInt)
    case (TLong, "toLong") => lhs.eval(ec)
    case (TLong, "toFloat") => AST.evalCompose[Long](ec, lhs)(_.toFloat)
    case (TLong, "toDouble") => AST.evalCompose[Long](ec, lhs)(_.toDouble)

    case (TFloat, "toInt") => AST.evalCompose[Float](ec, lhs)(_.toInt)
    case (TFloat, "toLong") => AST.evalCompose[Float](ec, lhs)(_.toLong)
    case (TFloat, "toFloat") => lhs.eval(ec)
    case (TFloat, "toDouble") => AST.evalCompose[Float](ec, lhs)(_.toDouble)

    case (TDouble, "toInt") => AST.evalCompose[Double](ec, lhs)(_.toInt)
    case (TDouble, "toLong") => AST.evalCompose[Double](ec, lhs)(_.toLong)
    case (TDouble, "toFloat") => AST.evalCompose[Double](ec, lhs)(_.toFloat)
    case (TDouble, "toDouble") => lhs.eval(ec)

    case (TString, "toInt") => AST.evalCompose[String](ec, lhs)(_.toInt)
    case (TString, "toLong") => AST.evalCompose[String](ec, lhs)(_.toLong)
    case (TString, "toFloat") => AST.evalCompose[String](ec, lhs)(_.toFloat)
    case (TString, "toDouble") => AST.evalCompose[String](ec, lhs)(_.toDouble)

    case (TInt, "abs") => AST.evalCompose[Int](ec, lhs)(_.abs)
    case (TLong, "abs") => AST.evalCompose[Long](ec, lhs)(_.abs)
    case (TFloat, "abs") => AST.evalCompose[Float](ec, lhs)(_.abs)
    case (TDouble, "abs") => AST.evalCompose[Double](ec, lhs)(_.abs)

    case (TInt, "signum") => AST.evalCompose[Int](ec, lhs)(_.signum)
    case (TLong, "signum") => AST.evalCompose[Long](ec, lhs)(_.signum)
    case (TFloat, "signum") => AST.evalCompose[Float](ec, lhs)(_.signum)
    case (TDouble, "signum") => AST.evalCompose[Double](ec, lhs)(_.signum)

    case (TString, "length") => AST.evalCompose[String](ec, lhs)(_.length)

    case (t: TArray, "length") => AST.evalCompose[Iterable[_]](ec, lhs)(_.size)
    case (t: TIterable, "size") => AST.evalCompose[Iterable[_]](ec, lhs)(_.size)
    case (t: TIterable, "isEmpty") => AST.evalCompose[Iterable[_]](ec, lhs)(_.isEmpty)
    case (t: TIterable, "toSet") => AST.evalCompose[Iterable[_]](ec, lhs)(_.toSet)
    case (t: TIterable, "toArray") => AST.evalCompose[Iterable[_]](ec, lhs)(_.toSeq)

    case (t: TDict, "size") => AST.evalCompose[Map[_, _]](ec, lhs)(_.size)
    case (t: TDict, "isEmpty") => AST.evalCompose[Map[_, _]](ec, lhs)(_.isEmpty)

    case (TArray(TInt), "sum") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Int]).sum)
    case (TArray(TLong), "sum") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Long]).sum)
    case (TArray(TFloat), "sum") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Float]).sum)
    case (TArray(TDouble), "sum") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Double]).sum)

    case (TArray(TInt), "min") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Int]).min)
    case (TArray(TLong), "min") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Long]).min)
    case (TArray(TFloat), "min") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Float]).min)
    case (TArray(TDouble), "min") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Double]).min)

    case (TArray(TInt), "max") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Int]).max)
    case (TArray(TLong), "max") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Long]).max)
    case (TArray(TFloat), "max") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Float]).max)
    case (TArray(TDouble), "max") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Double]).max)

    case (TSet(TInt), "sum") =>
      AST.evalCompose[Set[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Int]).sum)
    case (TSet(TLong), "sum") =>
      AST.evalCompose[Set[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Long]).sum)
    case (TSet(TFloat), "sum") =>
      AST.evalCompose[Set[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Float]).sum)
    case (TSet(TDouble), "sum") =>
      AST.evalCompose[Set[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Double]).sum)

    case (TSet(TInt), "min") =>
      AST.evalCompose[Set[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Int]).min)
    case (TSet(TLong), "min") =>
      AST.evalCompose[Set[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Long]).min)
    case (TSet(TFloat), "min") =>
      AST.evalCompose[Set[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Float]).min)
    case (TSet(TDouble), "min") =>
      AST.evalCompose[Set[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Double]).min)

    case (TSet(TInt), "max") =>
      AST.evalCompose[Set[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Int]).max)
    case (TSet(TLong), "max") =>
      AST.evalCompose[Set[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Long]).max)
    case (TSet(TFloat), "max") =>
      AST.evalCompose[Set[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Float]).max)
    case (TSet(TDouble), "max") =>
      AST.evalCompose[Set[_]](ec, lhs)(_.filter(x => x != null).map(_.asInstanceOf[Double]).max)

    case (TArray(elementType), "head") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.head)
    case (t@TArray(elementType), "tail") =>
      AST.evalCompose[IndexedSeq[_]](ec, lhs)(_.tail)
  }
}

case class ArrayConstructor(posn: Position, elements: Array[AST]) extends AST(posn, elements) {
  override def typecheckThis(ec: EvalContext): Type = {
    if (elements.isEmpty)
      parseError("invalid array constructor: no values")
    elements.foreach(_.typecheck(ec))
    val types: Set[Type] = elements.map(_.`type`)
      .map {
        case t: Type => t
        case bt => parseError(s"invalid array element found: `$bt'")
      }
      .toSet
    if (types.size == 1)
      TArray(types.head)
    else if (types.forall(_.isInstanceOf[TNumeric])) {
      TArray(TNumeric.promoteNumeric(types.map(_.asInstanceOf[TNumeric])))
    }
    else
      parseError(s"declared array elements must be the same type (or numeric)." +
        s"\n  Found: [${ elements.map(_.`type`).mkString(", ") }]")
  }

  def eval(ec: EvalContext): () => Any = {
    val f = elements.map(_.eval(ec))
    val elementType = `type`.asInstanceOf[TArray].elementType
    if (elementType.isInstanceOf[TNumeric]) {
      val types = elements.map(_.`type`.asInstanceOf[TNumeric])
      () => (types, f.map(_ ())).zipped.map { case (t, a) =>
        (elementType: @unchecked) match {
          case TDouble => t.makeDouble(a)
          case TFloat => a
          case TLong => t.asInstanceOf[TIntegral].makeLong(a)
          case TInt => a
        }
      }: IndexedSeq[Any]
    } else
      () => f.map(_ ()): IndexedSeq[Any]

  }
}

case class StructConstructor(posn: Position, names: Array[String], elements: Array[AST]) extends AST(posn, elements) {
  override def typecheckThis(ec: EvalContext): Type = {
    if (elements.isEmpty)
      parseError("Hail does not currently support declaring empty structs.")
    elements.foreach(_.typecheck(ec))
    val types = elements.map(_.`type`)
      .map {
        case t: Type => t
        case bt => parseError(s"invalid array element found: `$bt'")
      }
    TStruct((names, types, names.indices).zipped.map { case (id, t, i) => Field(id, t, i) })
  }

  def eval(ec: EvalContext): () => Any = {
    val f = elements.map(_.eval(ec))
    () => Annotation.fromSeq(f.map(_ ()))
  }
}

case class Lambda(posn: Position, param: String, body: AST) extends AST(posn, body) {
  def typecheck(): BaseType = parseError("non-function context")

  def eval(ec: EvalContext): () => Any = throw new UnsupportedOperationException
}

case class Apply(posn: Position, fn: String, args: Array[AST]) extends AST(posn, args) {
  var store: Any = null

  override def typecheckThis(): BaseType = {
    (fn, args) match {
      case ("isMissing", Array(a)) =>
        if (!a.`type`.isInstanceOf[Type])
          parseError(s"Got invalid argument `${ a.`type` } to function `$fn'")
        TBoolean

      case ("isDefined", Array(a)) =>
        if (!a.`type`.isInstanceOf[Type])
          parseError(s"Got invalid argument `${ a.`type` } to function `$fn'")
        TBoolean

      case ("str", Array(a)) =>
        if (!a.`type`.isInstanceOf[Type])
          parseError(s"Got invalid argument `${ a.`type` } to function `$fn'")
        TString

      case ("json", Array(a)) =>
        if (!a.`type`.isInstanceOf[Type])
          parseError(s"Got invalid argument `${ a.`type` } to function `$fn'")
        TString

      case ("Variant", _) =>
        args.map(_.`type`) match {
          case Array(TString) => TVariant
          case Array(TString, TInt, TString, TString) => TVariant
          case Array(TString, TInt, TString, TArray(TString)) => TVariant
          case other => parseError(
            s"""Got invalid arguments to variant constructor: (${ other.mkString(", ") })
                |  Acceptable formats:
                |    (String) for CHR:POS:REF:ALT or CHR:POS:REF:ALT1,ALT2,...ALTN
                |    (String, Int, String, String) for (chr, pos, ref, alt)
                |    (String, Int, String, Array[String]) for (chr, pos, ref, alts)""".stripMargin)
        }

      case ("Locus", _) =>
        args.map(_.`type`) match {
          case Array(TString) => TLocus
          case Array(TString, TInt) => TLocus
          case other => parseError(
            s"""Got invalid arguments to locus constructor: (${ other.mkString(", ") })
                |  Acceptable formats:
                |    (String) for CHR:POS
                |    (String, Int) for (chr, pos)""".stripMargin)
        }

      case ("Interval", _) =>
        args.map(_.`type`) match {
          case (Array(TLocus, TLocus)) => TInterval
          case other => parseError(
            s"""Got invalid arguments to interval constructor: (${ other.mkString(", ") })
                |  Acceptable format:
                |    (Locus, Locus) for (start, end)""".stripMargin)
        }


      case ("hwe", _) =>
        args.map(_.`type`) match {
          case Array(TInt, TInt, TInt) => TStruct(("rExpectedHetFrequency", TDouble), ("pHWE", TDouble))
          case other =>
            val nArgs = other.length
            parseError(
              s"""method `hwe' expects 3 arguments of type Int, e.g. hwe(90,10,1)
                  |  Found $nArgs ${ plural(nArgs, "argument") }${
                if (nArgs > 0)
                  s"of ${ plural(nArgs, "type") } [${ other.mkString(", ") }]"
                else ""
              }""".stripMargin)
        }

      case ("fet", rhs) =>
        rhs.map(_.`type`) match {
          case Array(TInt, TInt, TInt, TInt) => TStruct(("pValue", TDouble), ("oddsRatio", TDouble), ("ci95Lower", TDouble), ("ci95Upper", TDouble))
          case other =>
            val nArgs = other.length
            parseError(
              s"""method `fet' expects 4 arguments of type Int, e.g. fet(5,100,0,1000)
                  |  Found $nArgs ${ plural(nArgs, "argument") }${
                if (nArgs > 0)
                  s" of ${ plural(nArgs, "type") } [${ other.mkString(", ") }]"
                else ""
              }""".stripMargin)
        }

      case ("merge", rhs) =>
        val (t1, t2) = args.map(_.`type`) match {
          case Array(t1: TStruct, t2: TStruct) => (t1, t2)
          case other => parseError(
            s"""invalid arguments to `$fn'
                |  Expected $fn(Struct, Struct), found $fn(${ other.mkString(", ") })""".stripMargin)
        }

        val (t, merger) = try {
          t1.merge(t2)
        } catch {
          case f: FatalException => parseError(
            s"""invalid arguments for method `$fn'
                |  ${ f.getMessage }""".stripMargin)
          case e: Throwable => parseError(
            s"""invalid arguments for method `$fn'
                |  ${ e.getClass.getName }: ${ e.getMessage }""".stripMargin)
        }

        store = merger
        t

      case ("isDefined" | "isMissing" | "str" | "json", _) => parseError(s"`$fn' takes one argument")

      case ("exp" | "log10" | "sqrt", _) => TDouble
        args.map(_.`type`) match {
          case Array(a: TNumeric) => TDouble
          case other =>
            parseError(
              s"""invalid arguments in call to $fn: ${ other.mkString(", ") }.
                  |  Expected exp(Double)""".stripMargin)
        }

      case ("pow", _) => TDouble
        args.map(_.`type`) match {
          case Array(a: TNumeric, b: TNumeric) => TDouble
          case other =>
            parseError(
              s"""invalid arguments in call to $fn: ${ other.mkString(", ") }.
                  |  Expected $fn(Double)""".stripMargin)
        }

      case ("log", _) => TDouble
        args.map(_.`type`) match {
          case Array(a: TNumeric) => TDouble
          case Array(a: TNumeric, b: TNumeric) => TDouble
          case other =>
            parseError(
              s"""invalid arguments in call to $fn: ${ other.mkString(", ") }.
                  |  Expected either:
                  |    $fn(Double)
                  |    $fn(Double, Double)""".stripMargin)
        }
    }
  }

  override def typecheck(ec: EvalContext) {
    fn match {
      case "index" =>
        if (args.length != 2)
          parseError(
            s"""invalid arguments for method `$fn'
                |  Expected 2 arguments: $fn(Array[Struct], identifiers...)
                |  Found ${ args.length } arguments""".stripMargin)
        args.head.typecheck(ec)
        val t = args.head.`type` match {
          case TArray(t: TStruct) => t
          case error => parseError(
            s"""invalid arguments for method `$fn'
                |  Expected Array[Struct] as first argument, found `$error'""".stripMargin)
        }
        val key = args(1) match {
          case SymRef(_, id) => id
          case other =>
            parseError(
              s"""invalid arguments for method `$fn'
                  |  Expected struct field identifier as the second argument, but found a `${ other.getClass.getSimpleName }' expression
                  |  Usage: $fn(Array[Struct], key identifier)""".stripMargin)
        }

        t.getOption(key) match {
          case Some(TString) =>
            val querier = t.query(key)
            val (newS, deleter) = t.delete(key)
            store = (querier, deleter)
            `type` = TDict(newS)
          case Some(other) => parseError(
            s"""invalid arguments for method `$fn'
                |  Expected key to be of type String, but field ${ prettyIdentifier(key) } had type `$other'""".stripMargin)
          case None => parseError(
            s"""invalid arguments for method `$fn'
                |  Struct did not contain the designated key `${ prettyIdentifier(key) }'""".stripMargin)
        }

      case "select" | "drop" =>
        val usage = s"""Usage: `$fn'(Struct, identifiers...)"""
        if (args.length < 2)
          parseError(
            s"""too few arguments for method `$fn'
                |  Expected 2 or more arguments: $fn(Struct, identifiers...)
                |  Found ${ args.length } ${ plural(args.length, "argument") }""".stripMargin)
        val (head, tail) = (args.head, args.tail)
        head.typecheck(ec)
        val struct = head.`type` match {
          case t: TStruct => t
          case other => parseError(
            s"""method `$fn' expects a Struct argument in the first position
                |  Expected: $fn(Struct, ...)
                |  Found: $fn($other, ...)""".stripMargin)
        }
        val identifiers = tail.map {
          case SymRef(_, id) => id
          case other =>
            parseError(
              s"""invalid arguments for method `$fn'
                  |  Expected struct field identifiers after the first position, but found a `${ other.getClass.getSimpleName }' expression""".stripMargin)
        }
        val duplicates = identifiers.duplicates()
        if (duplicates.nonEmpty)
          parseError(
            s"""invalid arguments for method `$fn'
                |  Duplicate ${ plural(duplicates.size, "identifier") } found: [ ${ duplicates.map(prettyIdentifier).mkString(", ") } ]""".stripMargin)

        val (tNew, filterer) = try {
          struct.filter(identifiers.toSet, include = fn == "select")
        } catch {
          case f: FatalException => parseError(
            s"""invalid arguments for method `$fn'
                |  ${ f.getMessage }""".stripMargin)
          case e: Throwable => parseError(
            s"""invalid arguments for method `$fn'
                |  ${ e.getClass.getName }: ${ e.getMessage }""".stripMargin)
        }

        store = filterer
        `type` = tNew

      case _ => super.typecheck(ec)
    }
  }

  def eval(ec: EvalContext): () => Any = ((fn, args): @unchecked) match {
    case ("isMissing", Array(a)) =>
      val f = a.eval(ec)
      () => f() == null

    case ("isDefined", Array(a)) =>
      val f = a.eval(ec)
      () => f() != null

    case ("str", Array(a)) =>
      val t = a.`type`.asInstanceOf[Type]
      val f = a.eval(ec)
      () => t.str(f())

    case ("json", Array(a)) =>
      val t = a.`type`.asInstanceOf[Type]
      val f = a.eval(ec)
      () => JsonMethods.compact(t.toJSON(f()))

    case ("Variant", _) =>
      args match {
        case Array(v) => AST.evalCompose[String](ec, v) { vString =>
          val Array(chr, pos, ref, alts) = vString.split(":")
          Variant(chr, pos.toInt, ref, alts.split(","))
        }
        case Array(chrAST, posAST, refAST, altAST) =>
          (altAST.`type`: @unchecked) match {
            case TString =>
              AST.evalCompose[String, Int, String, String](ec, chrAST, posAST, refAST, altAST) {
                case (chr, pos, ref, alt) =>
                  Variant(chr, pos, ref, alt)
              }
            case TArray(_) => AST.evalCompose[String, Int, String, IndexedSeq[_]](ec, chrAST, posAST, refAST, altAST) {
              case (chr, pos, ref, alts) =>
                Variant(chr, pos, ref, alts.map(_.asInstanceOf[String]).toArray)
            }
          }
      }

    case ("Locus", _) =>
      args match {
        case Array(locusAST) => AST.evalCompose[String](ec, locusAST) { locusString =>
          val Array(chr, pos) = locusString.split(":")
          Locus(chr, pos.toInt)
        }
        case Array(chrAST, posAST) => AST.evalCompose[String, Int](ec, chrAST, posAST) { case (chr, pos) =>
          Locus(chr, pos)
        }
      }

    case ("Interval", Array(start, end)) =>
      AST.evalCompose[Locus, Locus](ec, start, end) { case (s, e) => Interval(s, e) }

    case ("hwe", Array(a, b, c)) =>
      AST.evalCompose[Int, Int, Int](ec, a, b, c) {
        case (nHomRef, nHet, nHomVar) =>
          if (nHomRef < 0 || nHet < 0 || nHomVar < 0)
            fatal(s"got invalid (negative) argument to function `hwe': hwe($nHomRef, $nHet, $nHomVar)")
          val n = nHomRef + nHet + nHomVar
          val nAB = nHet
          val nA = nAB + 2 * nHomRef.min(nHomVar)

          val LH = LeveneHaldane(n, nA)
          Annotation(divOption(LH.getNumericalMean, n).orNull, LH.exactMidP(nAB))
      }

    case ("fet", Array(a, b, c, d)) =>
      AST.evalCompose[Int, Int, Int, Int](ec, a, b, c, d) { case (c1, c2, c3, c4) =>
        if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0)
          fatal(s"got invalid argument to function `fet': fet($c1, $c2, $c3, $c4)")
        val fet = FisherExactTest(c1, c2, c3, c4)
        Annotation(fet(0).orNull, fet(1).orNull, fet(2).orNull, fet(3).orNull)
      }

    case ("merge", Array(struct1, struct2)) =>
      val f1 = struct1.eval(ec)
      val f2 = struct2.eval(ec)
      val merger = store.asInstanceOf[Merger]
      () => {
        merger(f1(), f2())
      }

    case ("select" | "drop", Array(struct, _ *)) =>
      val filterer = store.asInstanceOf[Deleter]
      AST.evalCompose[Annotation](ec, struct) { s =>
        filterer(s)
      }

    case ("index", Array(structArray, _)) =>
      val (querier, deleter) = store.asInstanceOf[(Querier, Deleter)]
      // FIXME: warn for duplicate keys?
      AST.evalCompose[IndexedSeq[_]](ec, structArray) { is =>
        is.filter(_ != null)
          .map(_.asInstanceOf[Row])
          .flatMap(r => querier(r).map(x => (x, deleter(r))))
          .toMap
      }

    case ("exp", Array(a)) =>
      AST.evalComposeNumeric[Double](ec, a)(x => math.exp(x))
    case ("pow", Array(a, b)) =>
      AST.evalComposeNumeric[Double, Double](ec, a, b)((b, x) => math.pow(b, x))
    case ("log", Array(a)) =>
      AST.evalComposeNumeric[Double](ec, a)(x => math.log(x))
    case ("log", Array(a, b)) =>
      AST.evalComposeNumeric[Double, Double](ec, a, b)((x, b) => math.log(x) / math.log(b))
    case ("log10", Array(a)) =>
      AST.evalComposeNumeric[Double](ec, a)(x => math.log10(x))
    case ("sqrt", Array(a)) =>
      AST.evalComposeNumeric[Double](ec, a)(x => math.sqrt(x))
  }

}

case class ApplyMethod(posn: Position, lhs: AST, method: String, args: Array[AST]) extends AST(posn, lhs +: args) {

  def getSymRefId(ast: AST): String = {
    ast match {
      case SymRef(_, id) => id
      case _ => ???
    }
  }

  override def typecheck(ec: EvalContext) {
    lhs.typecheck(ec)
    (lhs.`type`, method, args) match {

      case (TString, "replace", rhs) => {
        lhs.typecheck(ec)
        rhs.foreach(_.typecheck(ec))
        rhs.map(_.`type`) match {
          case Array(TString, TString) => TString
          case other =>
            val nArgs = other.length
            parseError(
              s"""method `$method' expects 2 arguments of type String, e.g. str.replace(" ", "_")
                  |  Found $nArgs ${ plural(nArgs, "argument") }${
                if (nArgs > 0)
                  s"of ${ plural(nArgs, "type") } [${ other.mkString(", ") }]"
                else ""
              }""".stripMargin)
        }
      }

      case (it: TIterable, "find", rhs) =>
        lhs.typecheck(ec)
        val (param, body) = rhs match {
          case Array(Lambda(_, p, b)) => (p, b)
          case _ => parseError(s"method `$method' expects a lambda function [param => Boolean], " +
            s"e.g. `x => x < 5' or `tc => tc.canonical == 1'")
        }
        body.typecheck(ec.copy(st = ec.st + ((param, (-1, it.elementType)))))
        if (body.`type` != TBoolean)
          parseError(s"method `$method' expects a lambda function [param => Boolean], got [param => ${ body.`type` }]")
        `type` = it.elementType

      case (it: TIterable, "map", rhs) =>
        lhs.typecheck(ec)
        val (param, body) = rhs match {
          case Array(Lambda(_, p, b)) => (p, b)
          case _ => parseError(s"method `$method' expects a lambda function [param => Any], " +
            s"e.g. `x => x * 10' or `tc => tc.gene_symbol'")
        }
        body.typecheck(ec.copy(st = ec.st + ((param, (-1, it.elementType)))))
        `type` = body.`type` match {
          case t: Type => it match {
            case TArray(_) => TArray(t)
            case TSet(_) => TSet(t)
          }
          case error =>
            parseError(s"method `$method' expects a lambda function [param => Any], got invalid mapping [param => $error]")
        }

      case (it: TIterable, "filter", rhs) =>
        lhs.typecheck(ec)
        val (param, body) = rhs match {
          case Array(Lambda(_, p, b)) => (p, b)
          case _ => parseError(s"method `$method' expects a lambda function [param => Boolean], " +
            s"e.g. `x => x < 5' or `tc => tc.canonical == 1'")
        }
        body.typecheck(ec.copy(st = ec.st + ((param, (-1, it.elementType)))))
        if (body.`type` != TBoolean)
          parseError(s"method `$method' expects a lambda function [param => Boolean], got [param => ${ body.`type` }]")
        `type` = it

      case (it: TIterable, "forall" | "exists", rhs) =>
        lhs.typecheck(ec)
        val (param, body) = rhs match {
          case Array(Lambda(_, p, b)) => (p, b)
          case _ => parseError(s"method `$method' expects a lambda function [param => Boolean], " +
            s"e.g. `x => x < 5' or `tc => tc.canonical == 1'")
        }
        body.typecheck(ec.copy(st = ec.st + ((param, (-1, it.elementType)))))
        if (body.`type` != TBoolean)
          parseError(s"method `$method' expects a lambda function [param => Boolean], got [param => ${ body.`type` }]")
        `type` = TBoolean

      case (arr: TArray, "sort", rhs) =>
        lhs.typecheck(ec)
        rhs match {
          case Array() =>
          case Array(Const(_, _, TBoolean)) =>
          case _ => parseError(s"method `$method' expects at most one Boolean parameter")
        }
        `type` = arr

      case (arr: TArray, "sortBy", rhs) =>
        lhs.typecheck(ec)
        val (param, body) = rhs match {
          case Array(Lambda(_, p, b)) => (p, b)
          case Array(Lambda(_, p, b), Const(_, _, TBoolean)) => (p, b)
          case _ => parseError(s"method `$method' expects a lambda function [param => T] and at most one Boolean parameter")
        }
        body.typecheck(ec.copy(st = ec.st + ((param, (-1, arr.elementType)))))
        if (!(body.`type`.isInstanceOf[TNumeric] || body.`type` == TString))
          parseError(s"method `$method' expects a lambda function [param => T] with T of string or numeric type, got [param => ${ body.`type` }]")
        `type` = arr

      case (TDict(elementType), "mapvalues", rhs) =>
        lhs.typecheck(ec)
        val (param, body) = rhs match {
          case Array(Lambda(_, p, b)) => (p, b)
          case _ => parseError(s"method `$method' expects a lambda function [param => Any], " +
            s"e.g. `x => x < 5' or `tc => tc.canonical'")
        }
        body.typecheck(ec.copy(st = ec.st + ((param, (-1, elementType)))))
        `type` = body.`type` match {
          case t: Type => TDict(t)
          case error =>
            parseError(s"method `$method' expects a lambda function [param => Any], got invalid mapping [param => $error]")
        }

      case (agg: TAggregable, "count", rhs) =>
        rhs.foreach(_.typecheck(agg.ec))
        val types = rhs.map(_.`type`)
          .toSeq

        if (types != Seq(TBoolean)) {
          //          val plural1 = if (expected.length > 1) "s" else ""
          val plural = if (types.length != 1) "s" else ""
          parseError(s"method `$method' expects 1 argument of type (Boolean), but got ${ types.length } argument$plural${
            if (types.nonEmpty) s" of type (${ types.mkString(", ") })."
            else "."
          }")
        }
        `type` = TLong

      case (agg: TAggregable, "fraction", rhs) =>
        rhs.foreach(_.typecheck(agg.ec))
        val types = rhs.map(_.`type`)
          .toSeq

        if (types != Seq(TBoolean)) {
          //          val plural1 = if (expected.length > 1) "s" else ""
          val plural = if (types.length != 1) "s" else ""
          parseError(s"method `$method' expects 1 argument of type (Boolean), but got ${ types.length } argument$plural${
            if (types.nonEmpty) s" of type (${ types.mkString(", ") })."
            else "."
          }")
        }
        `type` = TDouble

      case (agg: TAggregable, "stats", rhs) =>
        rhs.foreach(_.typecheck(agg.ec))
        val types = rhs.map(_.`type`)
          .toSeq

        if (types.length != 1 || !types.head.isInstanceOf[TNumeric]) {
          val plural = if (types.length != 1) "s" else ""
          parseError(s"method `$method' expects 1 argument of types (Numeric), but got ${ types.length } argument$plural${
            if (types.nonEmpty) s" of type (${ types.mkString(", ") })."
            else "."
          }")
        }
        val t = types.head.asInstanceOf[Type]

        val sumT = t match {
          case tint: TIntegral => TLong
          case _ => TDouble
        }
        `type` = TStruct(("mean", TDouble), ("stdev", TDouble), ("min", t),
          ("max", t), ("nNotMissing", TLong), ("sum", sumT))

      case (agg: TAggregable, "statsif", rhs) =>
        rhs.foreach(_.typecheck(agg.ec))
        val types = rhs.map(_.`type`)
          .toSeq

        if (types.length != 2 || types.head != TBoolean || !types(1).isInstanceOf[TNumeric]) {
          val plural = if (types.length != 1) "s" else ""
          parseError(s"method `$method' expects 2 arguments of types (Boolean, Numeric), but got ${ types.length } argument$plural${
            if (types.nonEmpty) s" of type (${ types.mkString(", ") })."
            else "."
          }")
        }
        val t = types(1).asInstanceOf[Type]

        val sumT = if (t.isInstanceOf[TIntegral])
          TLong
        else
          TDouble
        `type` = TStruct(("mean", TDouble), ("stdev", TDouble), ("min", t),
          ("max", t), ("nNotMissing", TLong), ("sum", sumT))

      case (agg: TAggregable, "collect", rhs) =>
        rhs.foreach(_.typecheck(agg.ec))
        val types = rhs.map(_.`type`)
          .toSeq

        if (types.length != 2 || types.head != TBoolean) {
          val plural = if (types.length != 1) "s" else ""
          parseError(s"method `$method' expected 2 arguments of types (Boolean, Any), but got ${ types.length } argument$plural${
            if (types.nonEmpty) s" of type (${ types.mkString(", ") })."
            else "."
          }")
        }
        val t = types(1) match {
          case tws: Type => tws
          case _ => parseError(s"method `$method' expects a standard type as its map argument, but got `${ types(1) }'")
        }

        `type` = TArray(t)

      case _ =>
        super.typecheck(ec)
    }
  }

  override def typecheckThis(): BaseType = {
    val rhsTypes = args.map(_.`type`)
    (lhs.`type`, method, rhsTypes) match {
      case (TArray(TString), "mkString", Array(TString)) => TString
      case (TSet(elementType), "contains", Array(t2)) =>
        if (elementType != t2)
          parseError(
            s"""method `contains' takes an argument of the same type as the set.
                |  Expected type `$elementType' for `Set[$elementType]', but found `$t2'""".stripMargin)
        TBoolean
      case (TDict(_), "contains", Array(TString)) => TBoolean
      case (TInterval, "contains", Array(TLocus)) => TBoolean
      case (TString, "split", Array(TString)) => TArray(TString)

      case (t: TNumeric, "min", Array(t2: TNumeric)) =>
        AST.promoteNumeric(t, t2)
      case (t: TNumeric, "max", Array(t2: TNumeric)) =>
        AST.promoteNumeric(t, t2)

      case (t, "orElse", Array(t2)) if t == t2 =>
        t

      case (t, _, _) =>
        parseError(s"`no matching signature for `$method(${ rhsTypes.mkString(", ") })' on `$t'")
    }
  }

  def eval(ec: EvalContext): () => Any = ((lhs.`type`, method, args): @unchecked) match {
    case (returnType, "find", Array(Lambda(_, param, body))) =>
      val localIdx = ec.a.length
      val localA = ec.a
      localA += null
      val bodyFn = body.eval(ec.copy(st = ec.st + (param -> (localIdx, returnType))))

      AST.evalCompose[Iterable[_]](ec, lhs) { s =>
        s.find { elt =>
          localA(localIdx) = elt
          val r = bodyFn()
          r != null && r.asInstanceOf[Boolean]
        }.orNull
      }

    case (returnType, "map", Array(Lambda(_, param, body))) =>
      val localIdx = ec.a.length
      val localA = ec.a
      localA += null
      val bodyFn = body.eval(ec.copy(st = ec.st + (param -> (localIdx, returnType))))

      (returnType: @unchecked) match {
        case TArray(_) =>
          AST.evalCompose[IndexedSeq[_]](ec, lhs) { is =>
            is.map { elt =>
              localA(localIdx) = elt
              bodyFn()
            }
          }
        case TSet(_) =>
          AST.evalCompose[Set[_]](ec, lhs) { s =>
            s.map { elt =>
              localA(localIdx) = elt
              bodyFn()
            }
          }
      }

    case (returnType, "filter", Array(Lambda(_, param, body))) =>
      val localIdx = ec.a.length
      val localA = ec.a
      localA += null
      val bodyFn = body.eval(ec.copy(st = ec.st + (param -> (localIdx, returnType))))

      (returnType: @unchecked) match {
        case TArray(_) =>
          AST.evalCompose[IndexedSeq[_]](ec, lhs) { is =>
            is.filter { elt =>
              localA(localIdx) = elt
              val r = bodyFn()
              r.asInstanceOf[Boolean]
            }
          }
        case TSet(_) =>
          AST.evalCompose[Set[_]](ec, lhs) { s =>
            s.filter { elt =>
              localA(localIdx) = elt
              val r = bodyFn()
              r.asInstanceOf[Boolean]
            }
          }
      }

    case (returnType, "forall", Array(Lambda(_, param, body))) =>
      val localIdx = ec.a.length
      val localA = ec.a
      localA += null
      val bodyFn = body.eval(ec.copy(st = ec.st + (param -> (localIdx, returnType))))

      AST.evalCompose[Iterable[_]](ec, lhs) { is =>
        is.forall { elt =>
          localA(localIdx) = elt
          val r = bodyFn()
          r.asInstanceOf[Boolean]
        }
      }

    case (returnType, "exists", Array(Lambda(_, param, body))) =>
      val localIdx = ec.a.length
      val localA = ec.a
      localA += null
      val bodyFn = body.eval(ec.copy(st = ec.st + (param -> (localIdx, returnType))))

      AST.evalCompose[Iterable[_]](ec, lhs) { is =>
        is.exists { elt =>
          localA(localIdx) = elt
          val r = bodyFn()
          r.asInstanceOf[Boolean]
        }
      }

    case (returnType, "sort", rhs) =>
      val ascending = (rhs: @unchecked) match {
        case Array() => true
        case Array(Const(_, asc, _)) => asc.asInstanceOf[Boolean]
      }
      val baseOrd = (returnType: @unchecked) match {
        case TArray(TDouble) => Ordering.Double
        case TArray(TFloat) => Ordering.Float
        case TArray(TLong) => Ordering.Long
        case TArray(TInt) => Ordering.Int
        case TArray(TString) => Ordering.String
      }
      val ord = extendOrderingToNull(
        if (ascending)
          baseOrd
        else
          baseOrd.reverse
      )
      AST.evalCompose[IndexedSeq[_]](ec, lhs) { arr =>
        arr.sorted(ord)
      }

    case (returnType, "sortBy", rhs) =>
      val ascending = (rhs: @unchecked) match {
        case Array(_) => true
        case Array(_, Const(_, asc, _)) => asc.asInstanceOf[Boolean]
      }
      val Lambda(_, param, body) = rhs(0)
      val baseOrd = (body.`type`: @unchecked) match {
        case TDouble => Ordering.Double
        case TFloat => Ordering.Float
        case TLong => Ordering.Long
        case TInt => Ordering.Int
        case TString => Ordering.String
      }
      val ord = extendOrderingToNull(
        if (ascending)
          baseOrd
        else
          baseOrd.reverse)
      val localIdx = ec.a.length
      val localA = ec.a
      localA += null
      val bodyFn = body.eval(ec.copy(st = ec.st + (param -> (localIdx, returnType))))
      AST.evalCompose[IndexedSeq[_]](ec, lhs) { arr =>
        arr.sortBy { elt =>
          localA(localIdx) = elt
          bodyFn()
        }(ord)
      }

    case (returnType, "mapvalues", Array(Lambda(_, param, body))) =>
      val localIdx = ec.a.length
      val localA = ec.a
      localA += null
      val bodyFn = body.eval(ec.copy(st = ec.st + (param -> (localIdx, returnType))))

      AST.evalCompose[Map[_, _]](ec, lhs) { m =>
        m.mapValues { elt =>
          localA(localIdx) = elt
          bodyFn()
        }.force
      }

    case (agg, "count", Array(predicate)) =>
      val newContext = agg.asInstanceOf[TAggregable].ec
      val localA = newContext.a
      val localIdx = localA.length
      localA += null
      val fn = predicate.eval(newContext)
      val localFunctions = newContext.aggregationFunctions
      val seqOp: (Any) => Any =
        (sum) => {
          val ret = fn().asInstanceOf[Boolean]
          val toAdd = if (ret)
            1
          else
            0
          sum.asInstanceOf[Long] + toAdd
        }
      val combOp: (Any, Any) => Any = _.asInstanceOf[Long] + _.asInstanceOf[Long]
      localFunctions += ((() => 0L, seqOp, combOp, localIdx))
      AST.evalCompose[Any](ec, lhs) { a => localA(localIdx) }

    case (agg, "fraction", Array(rhs)) =>
      val newContext = agg.asInstanceOf[TAggregable].ec
      val localA = newContext.a
      val localIdx = localA.length
      localA += null
      val fn = rhs.eval(newContext)
      val localFunctions = newContext.aggregationFunctions
      val (zv, seqOp, combOp) = {
        val so = (sum: Any) => {
          val counts = sum.asInstanceOf[(Long, Long)]
          val ret = fn().asInstanceOf[Boolean]
          if (ret)
            (counts._1 + 1, counts._2 + 1)
          else
            (counts._1, counts._2 + 1)
        }
        val co: (Any, Any) => Any = (left: Any, right: Any) => {
          val lh = left.asInstanceOf[(Long, Long)]
          val rh = right.asInstanceOf[(Long, Long)]
          (lh._1 + rh._1, lh._2 + rh._2)
        }
        (() => (0L, 0L), so, co)
      }
      localFunctions += ((zv, seqOp, combOp, localIdx))
      AST.evalCompose[Any](ec, lhs) { a =>
        val (num: Long, denom: Long) = localA(localIdx)
        divNull(num.toDouble, denom)
      }


    case (agg, "stats", Array(rhs)) =>
      val newContext = agg.asInstanceOf[TAggregable].ec
      val localA = newContext.a
      val localIdx = localA.length
      localA += null
      val fn = rhs.eval(newContext)
      val localFunctions = newContext.aggregationFunctions

      val localType = rhs.`type`.asInstanceOf[TNumeric]
      val seqOp = (a: Any) => {
        val query = fn()
        val sc = a.asInstanceOf[StatCounter]
        if (query != null)
          localType match {
            case TInt => sc.merge(query.asInstanceOf[Int])
            case TLong => sc.merge(query.asInstanceOf[Long])
            case TFloat => sc.merge(query.asInstanceOf[Float])
            case TDouble => sc.merge(query.asInstanceOf[Double])
          }
        else
          sc
      }

      val combOp = (a: Any, b: Any) => a.asInstanceOf[StatCounter].merge(b.asInstanceOf[StatCounter])

      val recast: (Double) => Any = localType match {
        case TInt => (d: Double) => d.round.toInt
        case TLong => (d: Double) => d.round
        case TFloat => (d: Double) => d.toFloat
        case TDouble => (d: Double) => d
      }

      val recast2: (Double) => Any =
        if (rhs.`type`.isInstanceOf[TIntegral])
          (d: Double) => d.round
        else
          (d: Double) => d

      val getOp = (a: Any) => {
        val statcounter = a.asInstanceOf[StatCounter]
        if (statcounter.count == 0)
          null
        else
          Annotation(statcounter.mean, statcounter.stdev, recast(statcounter.min),
            recast(statcounter.max), statcounter.count, recast2(statcounter.sum))
      }

      localFunctions += ((() => new StatCounter, seqOp, combOp, localIdx))
      AST.evalCompose[Any](ec, lhs) { a => getOp(localA(localIdx)) }

    case (returnType, "statsif", Array(condition, computation)) =>
      val newContext = lhs.`type`.asInstanceOf[TAggregable].ec
      val localA = newContext.a
      val localIdx = localA.length
      localA += null
      val conditionFn = condition.eval(newContext)
      val fn = computation.eval(newContext)
      val localFunctions = newContext.aggregationFunctions

      val localType = computation.`type`.asInstanceOf[TNumeric]
      val seqOp = (a: Any) => {
        val sc = a.asInstanceOf[StatCounter]
        if (conditionFn().asInstanceOf[Boolean]) {
          val query = fn()
          if (query != null)
            localType match {
              case TInt => sc.merge(query.asInstanceOf[Int])
              case TLong => sc.merge(query.asInstanceOf[Long])
              case TFloat => sc.merge(query.asInstanceOf[Float])
              case TDouble => sc.merge(query.asInstanceOf[Double])
            }
          else sc
        } else sc
      }

      val combOp = (a: Any, b: Any) => a.asInstanceOf[StatCounter].merge(b.asInstanceOf[StatCounter])

      val recast: (Double) => Any = localType match {
        case TInt => (d: Double) => d.round.toInt
        case TLong => (d: Double) => d.round
        case TFloat => (d: Double) => d.toFloat
        case TDouble => (d: Double) => d
      }

      val recast2: (Double) => Any =
        if (localType.isInstanceOf[TIntegral])
          (d: Double) => d.round
        else
          (d: Double) => d

      val getOp = (a: Any) => {
        val statcounter = a.asInstanceOf[StatCounter]
        if (statcounter.count == 0)
          null
        else
          Annotation(statcounter.mean, statcounter.stdev, recast(statcounter.min),
            recast(statcounter.max), statcounter.count, recast2(statcounter.sum))
      }

      localFunctions += ((() => new StatCounter, seqOp, combOp, localIdx))
      AST.evalCompose[Any](ec, lhs) { a => getOp(localA(localIdx)) }

    case (returnType, "collect", Array(condition, computation)) =>
      val newContext = lhs.`type`.asInstanceOf[TAggregable].ec
      val localIdx = newContext.a.length
      val localA = newContext.a
      localA += null
      val conditionFn = condition.eval(newContext)
      val fn = computation.eval(newContext)
      val localFunctions = newContext.aggregationFunctions
      val seqOp: (Any) => Any =
        (arr) => {
          val ab = arr.asInstanceOf[ArrayBuffer[Any]]
          if (conditionFn().asInstanceOf[Boolean]) {
            val comp = fn()
            if (comp != null)
              ab += comp
          }
          ab
        }
      val combOp: (Any, Any) => Any = (a, b) => {
        val ab1 = a.asInstanceOf[ArrayBuffer[Any]]
        val ab2 = b.asInstanceOf[ArrayBuffer[Any]]
        ab1 ++= ab2
        ab1
      }

      localFunctions += ((() => new ArrayBuffer[Any], seqOp, combOp, localIdx))
      AST.evalCompose[Any](ec, lhs) { a => localA(localIdx).asInstanceOf[ArrayBuffer[Any]].toIndexedSeq }

    case (_, "orElse", Array(a)) =>
      val f1 = lhs.eval(ec)
      val f2 = a.eval(ec)
      () => {
        val v = f1()
        if (v == null)
          f2()
        else
          v
      }

    case (TString, "replace", Array(a, b)) =>
      AST.evalCompose[String, String, String](ec, lhs, a, b) { case (str, pattern1, pattern2) =>
        str.replaceAll(pattern1, pattern2)
      }

    case (TArray(elementType), "mkString", Array(a)) =>
      AST.evalCompose[IndexedSeq[String], String](ec, lhs, a) { case (s, t) => s.map(elementType.str).mkString(t) }
    case (TSet(elementType), "contains", Array(a)) =>
      AST.evalCompose[Set[Any], Any](ec, lhs, a) { case (a, x) => a.contains(x) }
    case (TSet(elementType), "mkString", Array(a)) =>
      AST.evalCompose[IndexedSeq[String], String](ec, lhs, a) { case (s, t) => s.map(elementType.str).mkString(t) }

    case (TDict(elementType), "contains", Array(a)) =>
      AST.evalCompose[Map[String, _], String](ec, lhs, a) { case (m, key) => m.contains(key) }

    case (TInterval, "contains", Array(l)) =>
      AST.evalCompose[Interval[Locus], Locus](ec, lhs, l) { case (interval, locus) => interval.contains(locus) }

    case (TString, "split", Array(a)) =>
      AST.evalCompose[String, String](ec, lhs, a) { case (s, p) => s.split(p): IndexedSeq[String] }

    case (TInt, "min", Array(a)) => AST.evalComposeNumeric[Int, Int](ec, lhs, a)(_ min _)
    case (TLong, "min", Array(a)) => AST.evalComposeNumeric[Long, Long](ec, lhs, a)(_ min _)
    case (TFloat, "min", Array(a)) => AST.evalComposeNumeric[Float, Float](ec, lhs, a)(_ min _)
    case (TDouble, "min", Array(a)) => AST.evalComposeNumeric[Double, Double](ec, lhs, a)(_ min _)

    case (TInt, "max", Array(a)) => AST.evalComposeNumeric[Int, Int](ec, lhs, a)(_ max _)
    case (TLong, "max", Array(a)) => AST.evalComposeNumeric[Long, Long](ec, lhs, a)(_ max _)
    case (TFloat, "max", Array(a)) => AST.evalComposeNumeric[Float, Float](ec, lhs, a)(_ max _)
    case (TDouble, "max", Array(a)) => AST.evalComposeNumeric[Double, Double](ec, lhs, a)(_ max _)
  }

}

case class Let(posn: Position, bindings: Array[(String, AST)], body: AST) extends AST(posn, bindings.map(_._2) :+ body) {

  def eval(ec: EvalContext): () => Any = {
    val indexb = new mutable.ArrayBuilder.ofInt
    val bindingfb = mutable.ArrayBuilder.make[() => Any]()

    var symTab2 = ec.st
    val localA = ec.a
    for ((id, v) <- bindings) {
      val i = localA.length
      localA += null
      bindingfb += v.eval(ec.copy(st = symTab2))
      indexb += i
      symTab2 = symTab2 + (id -> (i, v.`type`))
    }

    val n = bindings.length
    val indices = indexb.result()
    val bindingfs = bindingfb.result()
    val bodyf = body.eval(ec.copy(st = symTab2))
    () => {
      for (i <- 0 until n)
        localA(indices(i)) = bindingfs(i)()
      bodyf()
    }
  }

  override def typecheck(ec: EvalContext) {
    var symTab2 = ec.st
    for ((id, v) <- bindings) {
      v.typecheck(ec.copy(st = symTab2))
      symTab2 = symTab2 + (id -> (-1, v.`type`))
    }
    body.typecheck(ec.copy(st = symTab2))

    `type` = body.`type`
  }
}

case class BinaryOp(posn: Position, lhs: AST, operation: String, rhs: AST) extends AST(posn, lhs, rhs) {
  def eval(ec: EvalContext): () => Any = ((operation, `type`): @unchecked) match {
    case ("+", TString) =>
      val lhsT = lhs.`type`.asInstanceOf[Type]
      val rhsT = rhs.`type`.asInstanceOf[Type]
      AST.evalCompose[Any, Any](ec, lhs, rhs) { (left, right) => lhsT.str(left) + rhsT.str(right) }
    case ("~", TBoolean) => AST.evalCompose[String, String](ec, lhs, rhs) { (s, t) =>
      s.r.findFirstIn(t).isDefined
    }

    case ("+" | "*" | "-" | "/", TArray(elementType)) =>
      val f: (Any, Any) => Any = (operation, elementType) match {
        case ("+", TDouble) => (l: Any, r: Any) =>
          if (l == null || r == null) null
          else DoubleNumericConversion.to(l) + DoubleNumericConversion.to(r)
        case ("+", TInt) => (l: Any, r: Any) =>
          if (l == null || r == null) null
          else IntNumericConversion.to(l) + IntNumericConversion.to(r)
        case ("+", TLong) => (l: Any, r: Any) =>
          if (l == null || r == null) null
          else LongNumericConversion.to(l) + LongNumericConversion.to(r)
        case ("+", TFloat) => (l: Any, r: Any) =>
          if (l == null || r == null) null
          else FloatNumericConversion.to(l) + FloatNumericConversion.to(r)
        case ("*", TDouble) => (l: Any, r: Any) =>
          if (l == null || r == null) null
          else DoubleNumericConversion.to(l) * DoubleNumericConversion.to(r)
        case ("*", TInt) => (l: Any, r: Any) =>
          if (l == null || r == null) null
          else IntNumericConversion.to(l) * IntNumericConversion.to(r)
        case ("*", TLong) => (l: Any, r: Any) =>
          if (l == null || r == null) null
          else LongNumericConversion.to(l) * LongNumericConversion.to(r)
        case ("*", TFloat) => (l: Any, r: Any) =>
          if (l == null || r == null) null
          else FloatNumericConversion.to(l) * FloatNumericConversion.to(r)
        case ("-", TDouble) => (l: Any, r: Any) =>
          if (l == null || r == null) null
          else DoubleNumericConversion.to(l) - DoubleNumericConversion.to(r)
        case ("-", TInt) => (l: Any, r: Any) =>
          if (l == null || r == null) null
          else IntNumericConversion.to(l) - IntNumericConversion.to(r)
        case ("-", TLong) => (l: Any, r: Any) =>
          if (l == null || r == null) null
          else LongNumericConversion.to(l) - LongNumericConversion.to(r)
        case ("-", TFloat) => (l: Any, r: Any) =>
          if (l == null || r == null) null
          else FloatNumericConversion.to(l) - FloatNumericConversion.to(r)
        case ("/", TDouble) => (l: Any, r: Any) =>
          if (l == null || r == null) null
          else DoubleNumericConversion.to(l) / DoubleNumericConversion.to(r)
      }

      ((lhs.`type`, rhs.`type`): @unchecked) match {
        case (t: TNumeric, TArray(_)) => AST.evalCompose[Any, IndexedSeq[_]](ec, lhs, rhs) { case (num, arr) =>
          arr.map(elem => f(num, elem))
        }
        case (TArray(_), t: TNumeric) => AST.evalCompose[IndexedSeq[_], Any](ec, lhs, rhs) { case (arr, num) =>
          arr.map(elem => f(elem, num))
        }
        case (TArray(_), TArray(_)) => AST.evalCompose[IndexedSeq[_], IndexedSeq[_]](ec, lhs, rhs) { case (left, right) =>
          if (left.length != right.length) ParserUtils.error(posn,
            s"""cannot apply operation `$operation' to arrays of unequal length
                |  Left: ${ left.length } elements
                |  Right: ${ right.length } elements""".stripMargin)
          (left, right).zipped.map(f)
        }
      }

    case ("||", TBoolean) =>
      val f1 = lhs.eval(ec)
      val f2 = rhs.eval(ec)
      () => {
        val x1 = f1()
        if (x1 != null) {
          if (x1.asInstanceOf[Boolean])
            true
          else
            f2()
        } else {
          val x2 = f2()
          if (x2 != null
            && x2.asInstanceOf[Boolean])
            true
          else
            null
        }
      }

    case ("&&", TBoolean) =>
      val f1 = lhs.eval(ec)
      val f2 = rhs.eval(ec)
      () => {
        val x = f1()
        if (x != null) {
          if (x.asInstanceOf[Boolean])
            f2()
          else
            false
        } else {
          val x2 = f2()
          if (x2 != null
            && !x2.asInstanceOf[Boolean])
            false
          else
            null
        }
      }

    case ("+", TInt) => AST.evalComposeNumeric[Int, Int](ec, lhs, rhs)(_ + _)
    case ("-", TInt) => AST.evalComposeNumeric[Int, Int](ec, lhs, rhs)(_ - _)
    case ("*", TInt) => AST.evalComposeNumeric[Int, Int](ec, lhs, rhs)(_ * _)
    case ("/", TInt) => AST.evalComposeNumeric[Double, Double](ec, lhs, rhs)(_ / _)
    case ("%", TInt) => AST.evalComposeNumeric[Int, Int](ec, lhs, rhs)(_ % _)

    case ("+", TLong) => AST.evalComposeNumeric[Long, Long](ec, lhs, rhs)(_ + _)
    case ("-", TLong) => AST.evalComposeNumeric[Long, Long](ec, lhs, rhs)(_ - _)
    case ("*", TLong) => AST.evalComposeNumeric[Long, Long](ec, lhs, rhs)(_ * _)
    case ("/", TLong) => AST.evalComposeNumeric[Double, Double](ec, lhs, rhs)(_ / _)
    case ("%", TLong) => AST.evalComposeNumeric[Long, Long](ec, lhs, rhs)(_ % _)

    case ("+", TFloat) => AST.evalComposeNumeric[Float, Float](ec, lhs, rhs)(_ + _)
    case ("-", TFloat) => AST.evalComposeNumeric[Float, Float](ec, lhs, rhs)(_ - _)
    case ("*", TFloat) => AST.evalComposeNumeric[Float, Float](ec, lhs, rhs)(_ * _)
    case ("/", TFloat) => AST.evalComposeNumeric[Float, Float](ec, lhs, rhs)(_ / _)

    case ("+", TDouble) => AST.evalComposeNumeric[Double, Double](ec, lhs, rhs)(_ + _)
    case ("-", TDouble) => AST.evalComposeNumeric[Double, Double](ec, lhs, rhs)(_ - _)
    case ("*", TDouble) => AST.evalComposeNumeric[Double, Double](ec, lhs, rhs)(_ * _)
    case ("/", TDouble) => AST.evalComposeNumeric[Double, Double](ec, lhs, rhs)(_ / _)

  }

  override def typecheckThis(): BaseType = (lhs.`type`, operation, rhs.`type`) match {
    case (t: Type, "+", TString) => TString
    case (TString, "+", t: Type) => TString
    case (TString, "~", TString) => TBoolean
    case (TBoolean, "||", TBoolean) => TBoolean
    case (TBoolean, "&&", TBoolean) => TBoolean
    case (lhsType: TIntegral, "%", rhsType: TIntegral) => AST.promoteNumeric(lhsType, rhsType)
    case (lhsType: TNumeric, "+", rhsType: TNumeric) => AST.promoteNumeric(lhsType, rhsType)
    case (lhsType: TNumeric, "-", rhsType: TNumeric) => AST.promoteNumeric(lhsType, rhsType)
    case (lhsType: TNumeric, "*", rhsType: TNumeric) => AST.promoteNumeric(lhsType, rhsType)
    case (lhsType: TNumeric, "/", rhsType: TNumeric) => TDouble

    case (TArray(lhsType: TNumeric), "+" | "-" | "*", TArray(rhsType: TNumeric)) =>
      TArray(AST.promoteNumeric(lhsType, rhsType))
    case (TArray(lhsType: TNumeric), "/", TArray(rhsType: TNumeric)) => TArray(TDouble)

    case (lhsType: TNumeric, "+" | "-" | "*", TArray(rhsType: TNumeric)) =>
      TArray(AST.promoteNumeric(lhsType, rhsType))
    case (lhsType: TNumeric, "/", TArray(rhsType: TNumeric)) => TArray(TDouble)
    case (TArray(lhsType: TNumeric), "+" | "-" | "*", rhsType: TNumeric) =>
      TArray(AST.promoteNumeric(lhsType, rhsType))
    case (TArray(lhsType: TNumeric), "/", rhsType: TNumeric) => TArray(TDouble)

    case (lhsType, _, rhsType) =>
      parseError(s"invalid arguments to `$operation': ($lhsType, $rhsType)")
  }
}

case class Comparison(posn: Position, lhs: AST, operation: String, rhs: AST) extends AST(posn, lhs, rhs) {
  var operandType: BaseType = null

  def eval(ec: EvalContext): () => Any = ((operation, operandType): @unchecked) match {
    case ("==", _) => AST.evalCompose[Any, Any](ec, lhs, rhs)(_ == _)
    case ("!=", _) => AST.evalCompose[Any, Any](ec, lhs, rhs)(_ != _)

    case ("<", TInt) => AST.evalComposeNumeric[Int, Int](ec, lhs, rhs)(_ < _)
    case ("<=", TInt) => AST.evalComposeNumeric[Int, Int](ec, lhs, rhs)(_ <= _)
    case (">", TInt) => AST.evalComposeNumeric[Int, Int](ec, lhs, rhs)(_ > _)
    case (">=", TInt) => AST.evalComposeNumeric[Int, Int](ec, lhs, rhs)(_ >= _)

    case ("<", TLong) => AST.evalComposeNumeric[Long, Long](ec, lhs, rhs)(_ < _)
    case ("<=", TLong) => AST.evalComposeNumeric[Long, Long](ec, lhs, rhs)(_ <= _)
    case (">", TLong) => AST.evalComposeNumeric[Long, Long](ec, lhs, rhs)(_ > _)
    case (">=", TLong) => AST.evalComposeNumeric[Long, Long](ec, lhs, rhs)(_ >= _)

    case ("<", TFloat) => AST.evalComposeNumeric[Float, Float](ec, lhs, rhs)(_ < _)
    case ("<=", TFloat) => AST.evalComposeNumeric[Float, Float](ec, lhs, rhs)(_ <= _)
    case (">", TFloat) => AST.evalComposeNumeric[Float, Float](ec, lhs, rhs)(_ > _)
    case (">=", TFloat) => AST.evalComposeNumeric[Float, Float](ec, lhs, rhs)(_ >= _)

    case ("<", TDouble) => AST.evalComposeNumeric[Double, Double](ec, lhs, rhs)(_ < _)
    case ("<=", TDouble) => AST.evalComposeNumeric[Double, Double](ec, lhs, rhs)(_ <= _)
    case (">", TDouble) => AST.evalComposeNumeric[Double, Double](ec, lhs, rhs)(_ > _)
    case (">=", TDouble) => AST.evalComposeNumeric[Double, Double](ec, lhs, rhs)(_ >= _)
  }

  override def typecheckThis(): BaseType = {
    operandType = (lhs.`type`, operation, rhs.`type`) match {
      case (lhsType: TNumeric, "==" | "!=" | "<=" | ">=" | "<" | ">", rhsType: TNumeric) =>
        AST.promoteNumeric(lhsType, rhsType)

      case (lhsType, "==" | "!=", rhsType) =>
        if (lhsType != rhsType)
          parseError(s"invalid comparison: `$lhsType' and `$rhsType', can only compare objects of similar type")
        else TBoolean

      case (lhsType, _, rhsType) =>
        parseError(s"invalid arguments to `$operation': ($lhsType, $rhsType)")
    }

    TBoolean
  }
}

case class UnaryOp(posn: Position, operation: String, operand: AST) extends AST(posn, operand) {
  def eval(ec: EvalContext): () => Any = ((operation, `type`): @unchecked) match {
    case ("-", TInt) => AST.evalComposeNumeric[Int](ec, operand)(-_)
    case ("-", TLong) => AST.evalComposeNumeric[Long](ec, operand)(-_)
    case ("-", TFloat) => AST.evalComposeNumeric[Float](ec, operand)(-_)
    case ("-", TDouble) => AST.evalComposeNumeric[Double](ec, operand)(-_)

    case ("!", TBoolean) => AST.evalCompose[Boolean](ec, operand)(!_)
  }

  override def typecheckThis(): BaseType = (operation, operand.`type`) match {
    case ("-", t: TNumeric) => AST.promoteNumeric(t)
    case ("!", TBoolean) => TBoolean

    case (_, t) =>
      parseError(s"invalid argument to unary `$operation': ${ t.toString }")
  }
}

case class IndexOp(posn: Position, f: AST, idx: AST) extends AST(posn, Array(f, idx)) {
  override def typecheckThis(): BaseType = (f.`type`, idx.`type`) match {
    case (TArray(elementType), TInt) => elementType
    case (TDict(elementType), TString) => elementType
    case (TString, TInt) => TChar

    case _ =>
      parseError(
        s""" invalid index expression: cannot index `${ f.`type` }' with type `${ idx.`type` }'
            |  Known index operations:
            |    Array indexed with Int: a[2]
            |    String indexed with Int: str[0] (Returns a character)
            |    Dict indexed with String: genes["PCSK9"]""".stripMargin)
  }

  def eval(ec: EvalContext): () => Any = ((f.`type`, idx.`type`): @unchecked) match {
    case (t: TArray, TInt) =>
      val localT = t
      val localPos = posn
      AST.evalCompose[IndexedSeq[_], Int](ec, f, idx)((a, i) =>
        try {
          if (i < 0)
            a(a.length + i)
          else
            a(i)
        } catch {
          case e: java.lang.IndexOutOfBoundsException =>
            ParserUtils.error(localPos,
              s"""Tried to access index [$i] on array ${ JsonMethods.compact(localT.toJSON(a)) } of length ${ a.length }
                  |  Hint: All arrays in Hail are zero-indexed (`array[0]' is the first element)
                  |  Hint: For accessing `A'-numbered info fields in split variants, `va.info.field[va.aIndex]' is correct""".stripMargin)
          case e: Throwable => throw e
        })

    case (TDict(_), TString) =>
      AST.evalCompose[Map[_, _], String](ec, f, idx)((d, k) =>
        d.asInstanceOf[Map[String, _]]
          .get(k)
          .orNull
      )

    case (TString, TInt) =>
      AST.evalCompose[String, Int](ec, f, idx)((s, i) => s(i).toString)
  }
}

case class SliceArray(posn: Position, f: AST, idx1: Option[AST], idx2: Option[AST]) extends AST(posn, Array(Some(f), idx1, idx2).flatten) {
  override def typecheckThis(): BaseType = f.`type` match {
    case (t: TArray) =>
      if (idx1.exists(_.`type` != TInt) || idx2.exists(_.`type` != TInt))
        parseError(
          s"""invalid slice expression
              |  Expect (array[start:end],  array[:end], or array[start:]) where start and end are integers
              |  Found [${ idx1.map(_.`type`).getOrElse("") }:${ idx2.map(_.`type`).getOrElse("") }]""".stripMargin)
      else
        t
    case _ => parseError(
      s"""invalid slice expression
          |  Only arrays can be sliced.  Found slice operation on type `${ f.`type` }'""".stripMargin)
  }

  def eval(ec: EvalContext): () => Any = {
    val i1 = idx1.getOrElse(Const(posn, 0, TInt))
    idx2 match {
      case (Some(i2)) =>
        AST.evalCompose[IndexedSeq[_], Int, Int](ec, f, i1, i2)((a, ind1, ind2) => a.slice(ind1, ind2))
      case (None) =>
        AST.evalCompose[IndexedSeq[_], Int](ec, f, i1)((a, ind1) => a.slice(ind1, a.length))
    }
  }
}

case class SymRef(posn: Position, symbol: String) extends AST(posn) {
  def eval(ec: EvalContext): () => Any = {
    val localI = ec.st(symbol)._1
    val localA = ec.a
    if (localI < 0)
      () => 0 // FIXME placeholder
    else
      () => localA(localI)
  }

  override def typecheckThis(ec: EvalContext): BaseType = {
    ec.st.get(symbol) match {
      case Some((_, t)) => t
      case None =>
        parseError(
          s"""symbol `$symbol' not found
              |  Available symbols:
              |    ${ ec.st.map { case (id, (_, t)) => s"${ prettyIdentifier(id) }: $t" }.mkString("\n    ") } """.stripMargin)
    }
  }
}

case class If(pos: Position, cond: AST, thenTree: AST, elseTree: AST)
  extends AST(pos, Array(cond, thenTree, elseTree)) {
  override def typecheckThis(ec: EvalContext): BaseType = {
    if (thenTree.`type` != elseTree.`type`)
      parseError(s"expected same-type `then' and `else' clause, got `${ thenTree.`type` }' and `${ elseTree.`type` }'")
    else
      thenTree.`type`
  }

  def eval(ec: EvalContext): () => Any = {
    val f1 = cond.eval(ec)
    val f2 = thenTree.eval(ec)
    val f3 = elseTree.eval(ec)
    () => {
      val c = f1()
      if (c != null) {
        if (c.asInstanceOf[Boolean])
          f2()
        else
          f3()
      } else
        null
    }
  }
}
