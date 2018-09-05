package is.hail.expr.ordering

import is.hail.annotations.Region
import is.hail.asm4s.{coerce, _}
import is.hail.expr.ir
import is.hail.expr.ir.EmitMethodBuilder
import is.hail.expr.types._
import is.hail.expr.types.physical._
import is.hail.utils._

object CodeOrdering {

  type Op = Int
  val compare: Op = 0
  val equiv: Op = 1
  val lt: Op = 2
  val lteq: Op = 3
  val gt: Op = 4
  val gteq: Op = 5
  val neq: Op = 6

  type F[R] = (Code[Region], (Code[Boolean], Code[_]), Code[Region], (Code[Boolean], Code[_])) => Code[R]

  def rowOrdering(t1: PStruct, t2: PStruct, mb: EmitMethodBuilder): CodeOrdering = new CodeOrdering {
    type T = Long

    val m1: LocalRef[Boolean] = mb.newLocal[Boolean]
    val m2: LocalRef[Boolean] = mb.newLocal[Boolean]

    val v1s: Array[LocalRef[_]] = t1.types.map(tf => mb.newLocal(ir.typeToTypeInfo(tf)))
    val v2s: Array[LocalRef[_]] = t2.types.map(tf => mb.newLocal(ir.typeToTypeInfo(tf)))

    def setup(i: Int)(rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long], missingGreatest: Boolean): Code[Unit] = {
      val tf1 = t1.types(i)
      val tf2 = t2.types(i)
      Code(
        m1 := t1.isFieldMissing(rx, x, i),
        m2 := t2.isFieldMissing(ry, y, i),
        v1s(i).storeAny(m1.mux(ir.defaultValue(tf1), rx.loadIRIntermediate(tf1)(t1.fieldOffset(x, i)))),
        v2s(i).storeAny(m2.mux(ir.defaultValue(tf2), ry.loadIRIntermediate(tf2)(t2.fieldOffset(y, i)))))
    }

    override def compareNonnull(rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long], missingGreatest: Boolean): Code[Int] = {
      val cmp = mb.newLocal[Int]

      val c = Array.tabulate(t1.size) { i =>
        val mbcmp = mb.getCodeOrdering[Int](t1.types(i), t2.types(i), CodeOrdering.compare, missingGreatest)
        Code(setup(i)(rx, x, ry, y, missingGreatest),
          mbcmp(rx, (m1, v1s(i)), ry, (m2, v2s(i))))
      }.foldRight[Code[Int]](cmp.load()) { (ci, cont) => cmp.ceq(0).mux(Code(cmp := ci, cont), cmp) }

      Code(cmp := 0, c)
    }

    override def ltNonnull(rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long], missingGreatest: Boolean): Code[Boolean] = {
      Array.tabulate(t1.size) { i =>
        val mblt = mb.getCodeOrdering[Boolean](t1.types(i), t2.types(i), CodeOrdering.lt, missingGreatest)
        val mbequiv = mb.getCodeOrdering[Boolean](t1.types(i), t2.types(i), CodeOrdering.equiv, missingGreatest)
        (Code(setup(i)(rx, x, ry, y, missingGreatest), mblt(rx, (m1, v1s(i)), ry, (m2, v2s(i)))),
          mbequiv(rx, (m1, v1s(i)), ry, (m2, v2s(i))))
      }.foldRight[Code[Boolean]](false) { case ((clt, ceq), cont) => clt || (ceq && cont) }
    }

    override def lteqNonnull(rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long], missingGreatest: Boolean): Code[Boolean] = {
      Array.tabulate(t1.size) { i =>
        val mblteq = mb.getCodeOrdering[Boolean](t1.types(i), t2.types(i), CodeOrdering.lteq, missingGreatest)
        val mbequiv = mb.getCodeOrdering[Boolean](t1.types(i), t2.types(i), CodeOrdering.equiv, missingGreatest)
        (Code(setup(i)(rx, x, ry, y, missingGreatest), mblteq(rx, (m1, v1s(i)), ry, (m2, v2s(i)))),
          mbequiv(rx, (m1, v1s(i)), ry, (m2, v2s(i))))
      }.foldRight[Code[Boolean]](true) { case ((clteq, ceq), cont) => clteq && (!ceq || cont) }
    }

    override def gtNonnull(rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long], missingGreatest: Boolean): Code[Boolean] = {
      Array.tabulate(t1.size) { i =>
        val mbgt = mb.getCodeOrdering[Boolean](t1.types(i), t2.types(i), CodeOrdering.gt, missingGreatest)
        val mbequiv = mb.getCodeOrdering[Boolean](t1.types(i), t2.types(i), CodeOrdering.equiv, missingGreatest)
        (Code(setup(i)(rx, x, ry, y, missingGreatest), mbgt(rx, (m1, v1s(i)), ry, (m2, v2s(i)))),
          mbequiv(rx, (m1, v1s(i)), ry, (m2, v2s(i))))
      }.foldRight[Code[Boolean]](false) { case ((cgt, ceq), cont) => cgt || (ceq && cont) }
    }

    override def gteqNonnull(rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long], missingGreatest: Boolean): Code[Boolean] = {
      Array.tabulate(t1.size) { i =>
        val mbgteq = mb.getCodeOrdering[Boolean](t1.types(i), t2.types(i), CodeOrdering.gteq, missingGreatest)
        val mbequiv = mb.getCodeOrdering[Boolean](t1.types(i), t2.types(i), CodeOrdering.equiv, missingGreatest)
        (Code(setup(i)(rx, x, ry, y, missingGreatest), mbgteq(rx, (m1, v1s(i)), ry, (m2, v2s(i)))),
          mbequiv(rx, (m1, v1s(i)), ry, (m2, v2s(i))))
      }.foldRight[Code[Boolean]](true) { case ((cgteq, ceq), cont) => cgteq && (!ceq || cont) }
    }

    override def equivNonnull(rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long], missingGreatest: Boolean): Code[Boolean] = {
      Array.tabulate(t1.size) { i =>
        val mbequiv = mb.getCodeOrdering[Boolean](t1.types(i), t2.types(i), CodeOrdering.equiv, missingGreatest)
        Code(setup(i)(rx, x, ry, y, missingGreatest),
          mbequiv(rx, (m1, v1s(i)), ry, (m2, v2s(i))))
      }.foldRight[Code[Boolean]](const(true))(_ && _)
    }
  }

  def intervalOrdering(t1: TInterval, t2: TInterval, mb: EmitMethodBuilder): CodeOrdering = new CodeOrdering {
    type T = Long
    val pti = ir.typeToTypeInfo(t1.pointType)
    val mp1: LocalRef[Boolean] = mb.newLocal[Boolean]
    val mp2: LocalRef[Boolean] = mb.newLocal[Boolean]
    val p1: LocalRef[_] = mb.newLocal(ir.typeToTypeInfo(t1.pointType))
    val p2: LocalRef[_] = mb.newLocal(ir.typeToTypeInfo(t2.pointType))

    def loadStart(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T]): Code[Unit] = {
      Code(
        mp1 := !t1.startDefined(rx, x),
        mp2 := !t2.startDefined(ry, y),
        p1.storeAny(mp1.mux(ir.defaultValue(t1.pointType), rx.loadIRIntermediate(t1.pointType)(t1.startOffset(x)))),
        p2.storeAny(mp2.mux(ir.defaultValue(t2.pointType), ry.loadIRIntermediate(t2.pointType)(t2.startOffset(y)))))
    }

    def loadEnd(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T]): Code[Unit] = {
      Code(
        mp1 := !t1.endDefined(rx, x),
        mp2 := !t2.endDefined(ry, y),
        p1.storeAny(mp1.mux(ir.defaultValue(t1.pointType), rx.loadIRIntermediate(t1.pointType)(t1.endOffset(x)))),
        p2.storeAny(mp2.mux(ir.defaultValue(t2.pointType), ry.loadIRIntermediate(t2.pointType)(t2.endOffset(y)))))
    }

    override def compareNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Int] = {
      val mbcmp = mb.getCodeOrdering[Int](t1.pointType, t2.pointType, CodeOrdering.compare, missingGreatest)

      val cmp = mb.newLocal[Int]
      Code(loadStart(rx, x, ry, y),
        cmp := mbcmp(rx, (mp1, p1), ry, (mp2, p2)),
        cmp.ceq(0).mux(
          Code(mp1 := t1.includeStart(rx, x),
            mp1.cne(t2.includeStart(ry, y)).mux(
              mp1.mux(-1, 1),
              Code(
                loadEnd(rx, x, ry, y),
                cmp := mbcmp(rx, (mp1, p1), ry, (mp2, p2)),
                cmp.ceq(0).mux(
                  Code(mp1 := t1.includeEnd(rx, x),
                    mp1.cne(t2.includeEnd(ry, y)).mux(mp1.mux(1, -1), 0)),
                  cmp)))),
          cmp))
    }

    override def equivNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] = {
      val mbeq = mb.getCodeOrdering[Boolean](t1.pointType, t2.pointType, CodeOrdering.equiv, missingGreatest)

      Code(loadStart(rx, x, ry, y), mbeq(rx, (mp1, p1), ry, (mp2, p2))) &&
        t1.includeStart(rx, x).ceq(t2.includeStart(ry, y)) &&
        Code(loadEnd(rx, x, ry, y), mbeq(rx, (mp1, p1), ry, (mp2, p2))) &&
        t1.includeEnd(rx, x).ceq(t2.includeEnd(ry, y))
    }

    override def ltNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] = {
      val mblt = mb.getCodeOrdering[Boolean](t1.pointType, t2.pointType, CodeOrdering.lt, missingGreatest)
      val mbeq = mb.getCodeOrdering[Boolean](t1.pointType, t2.pointType, CodeOrdering.equiv, missingGreatest)

      Code(loadStart(rx, x, ry, y), mblt(rx, (mp1, p1), ry, (mp2, p2))) || (
        mbeq(rx, (mp1, p1), ry, (mp2, p2)) && (
          Code(mp1 := t1.includeStart(rx, x), mp2 := t2.includeStart(ry, y), mp1 && !mp2) || (mp1.ceq(mp2) && (
            Code(loadEnd(rx, x, ry, y), mblt(rx, (mp1, p1), ry, (mp2, p2))) || (
              mbeq(rx, (mp1, p1), ry, (mp2, p2)) &&
                !t1.includeEnd(rx, x) && t2.includeEnd(ry, y))))))
    }

    override def lteqNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] = {
      val mblteq = mb.getCodeOrdering[Boolean](t1.pointType, t2.pointType, CodeOrdering.lteq, missingGreatest)
      val mbeq = mb.getCodeOrdering[Boolean](t1.pointType, t2.pointType, CodeOrdering.equiv, missingGreatest)

      Code(loadStart(rx, x, ry, y), mblteq(rx, (mp1, p1), ry, (mp2, p2))) && (
        !mbeq(rx, (mp1, p1), ry, (mp2, p2)) || (// if not equal, then lt
          Code(mp1 := t1.includeStart(rx, x), mp2 := t2.includeStart(ry, y), mp1 && !mp2) || (mp1.ceq(mp2) && (
            Code(loadEnd(rx, x, ry, y), mblteq(rx, (mp1, p1), ry, (mp2, p2))) && (
              !mbeq(rx, (mp1, p1), ry, (mp2, p2)) ||
                !t1.includeEnd(rx, x) || t2.includeEnd(ry, y))))))
    }

    override def gtNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] = {
      val mbgt = mb.getCodeOrdering[Boolean](t1.pointType, t2.pointType, CodeOrdering.gt, missingGreatest)
      val mbeq = mb.getCodeOrdering[Boolean](t1.pointType, t2.pointType, CodeOrdering.equiv, missingGreatest)

      Code(loadStart(rx, x, ry, y), mbgt(rx, (mp1, p1), ry, (mp2, p2))) || (
        mbeq(rx, (mp1, p1), ry, (mp2, p2)) && (
          Code(mp1 := t1.includeStart(rx, x), mp2 := t2.includeStart(ry, y), !mp1 && mp2) || (mp1.ceq(mp2) && (
            Code(loadEnd(rx, x, ry, y), mbgt(rx, (mp1, p1), ry, (mp2, p2))) || (
              mbeq(rx, (mp1, p1), ry, (mp2, p2)) &&
                t1.includeEnd(rx, x) && !t2.includeEnd(ry, y))))))
    }

    override def gteqNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] = {
      val mbgteq = mb.getCodeOrdering[Boolean](t1.pointType, t2.pointType, CodeOrdering.gteq, missingGreatest)
      val mbeq = mb.getCodeOrdering[Boolean](t1.pointType, t2.pointType, CodeOrdering.equiv, missingGreatest)

      Code(loadStart(rx, x, ry, y), mbgteq(rx, (mp1, p1), ry, (mp2, p2))) && (
        !mbeq(rx, (mp1, p1), ry, (mp2, p2)) || (// if not equal, then lt
          Code(mp1 := t1.includeStart(rx, x), mp2 := t2.includeStart(ry, y), !mp1 && mp2) || (mp1.ceq(mp2) && (
            Code(loadEnd(rx, x, ry, y), mbgteq(rx, (mp1, p1), ry, (mp2, p2))) && (
              !mbeq(rx, (mp1, p1), ry, (mp2, p2)) ||
                t1.includeEnd(rx, x) || !t2.includeEnd(ry, y))))))
    }
  }

  def mapOrdering(t1: TDict, t2: TDict, mb: EmitMethodBuilder): CodeOrdering =
    iterableOrdering(TArray(t1.elementType, t1.required), TArray(t2.elementType, t2.required), mb)

  def setOrdering(t1: TSet, t2: TSet, mb: EmitMethodBuilder): CodeOrdering =
    iterableOrdering(TArray(t1.elementType, t1.required), TArray(t2.elementType, t2.required), mb)

  def apply(mb: MethodBuilder, virtualType: Type, left: PType, right: PType): CodeOrdering = {
    (virtualType, left, right) match {
      case (TInt32(_), l: PInt32, r: PInt32) =>
        new CodeOrdering {
          type T = Int

          def compareNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Int] =
            Code.invokeStatic[java.lang.Integer, Int, Int, Int]("compare", x, y)

          override def ltNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x < y

          override def lteqNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x <= y

          override def gtNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x > y

          override def gteqNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x >= y

          override def equivNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x.ceq(y)
        }
      case (TInt64(_), l: PInt64, r: PInt64) =>
        new CodeOrdering {
          type T = Long

          def compareNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Int] =
            Code.invokeStatic[java.lang.Long, Long, Long, Int]("compare", x, y)

          override def ltNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x < y

          override def lteqNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x <= y

          override def gtNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x > y

          override def gteqNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x >= y

          override def equivNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x.ceq(y)
        }
      case (TFloat32(_), l: PFloat32, r: PFloat32) =>
        new CodeOrdering {
          type T = Float

          def compareNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Int] =
            Code.invokeStatic[java.lang.Float, Float, Float, Int]("compare", x, y)

          override def ltNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x < y

          override def lteqNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x <= y

          override def gtNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x > y

          override def gteqNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x >= y

          override def equivNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x.ceq(y)
        }
      case (TFloat64(_), l: PFloat64, r: PFloat64) =>
        new CodeOrdering {
          type T = Double

          def compareNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Int] =
            Code.invokeStatic[java.lang.Double, Double, Double, Int]("compare", x, y)

          override def ltNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x < y

          override def lteqNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x <= y

          override def gtNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x > y

          override def gteqNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x >= y

          override def equivNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
            x.ceq(y)
        }
      case (TBoolean(_), l: PBoolean, r: PBoolean) =>
        new CodeOrdering {
          type T = Boolean

          def compareNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Int] =
            Code.invokeStatic[java.lang.Boolean, Boolean, Boolean, Int]("compare", x, y)
        }
      case (TBinary(_) | TString(_), l: PBinary, r: PBinary) =>
        new CodeOrdering {
          type T = Long

          def compareNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Int] = {
            val l1 = mb.newLocal[Int]
            val l2 = mb.newLocal[Int]
            val lim = mb.newLocal[Int]
            val i = mb.newLocal[Int]
            val cmp = mb.newLocal[Int]

            Code(
              l1 := l.loadLength(rx, x),
              l2 := r.loadLength(ry, y),
              lim := (l1 < l2).mux(l1, l2),
              i := 0,
              cmp := 0,
              Code.whileLoop(cmp.ceq(0) && i < lim,
                cmp := Code.invokeStatic[java.lang.Byte, Byte, Byte, Int]("compare",
                  rx.loadByte(l.bytesOffset(x) + i.toL),
                  ry.loadByte(r.bytesOffset(y) + i.toL)),
                i += 1),
              cmp.ceq(0).mux(Code.invokeStatic[java.lang.Integer, Int, Int, Int]("compare", l1, l2), cmp))
          }
        }
      case (TArray(elementType, _), l: PArray, r: PArray) =>
        new CodeOrdering {
          type T = Long
          val lord: CodeOrdering = CodeOrdering(mb, TInt32(), PCanonicalInt32, PCanonicalInt32)
          val ord: CodeOrdering = CodeOrdering(mb, elementType, l.elementType, r.elementType)
          val len1: LocalRef[Int] = mb.newLocal[Int]
          val len2: LocalRef[Int] = mb.newLocal[Int]
          val lim: LocalRef[Int] = mb.newLocal[Int]
          val i: LocalRef[Int] = mb.newLocal[Int]
          val m1: LocalRef[Boolean] = mb.newLocal[Boolean]
          val v1: LocalRef[ord.T] = mb.newLocal(ir.typeToTypeInfo(elementType)).asInstanceOf[LocalRef[ord.T]]
          val m2: LocalRef[Boolean] = mb.newLocal[Boolean]
          val v2: LocalRef[ord.T] = mb.newLocal(ir.typeToTypeInfo(elementType)).asInstanceOf[LocalRef[ord.T]]
          val eq: LocalRef[Boolean] = mb.newLocal[Boolean]

          def loop(cmp: Code[Unit], loopCond: Code[Boolean])
            (rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long]): Code[Unit] = {
            Code(
              i := 0,
              len1 := l.loadLength(rx, x),
              len2 := r.loadLength(ry, y),
              lim := (len1 < len2).mux(len1, len2),
              Code.whileLoop(loopCond && i < lim,
                m1 := l.isElementMissing(rx, x, i),
                v1.storeAny(rx.loadIRIntermediate(t1.elementType)(t1.elementOffset(x, len1, i))),
                m2 := r.isElementMissing(ry, y, i),
                v2.storeAny(ry.loadIRIntermediate(t2.elementType)(t2.elementOffset(y, len2, i))),
                cmp, i += 1))
          }

          override def compareNonnull(rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long], missingGreatest: Boolean): Code[Int] = {
            val mbcmp = mb.getCodeOrdering[Int](t1.elementType, t2.elementType, CodeOrdering.compare, missingGreatest)
            val cmp = mb.newLocal[Int]

            Code(cmp := 0,
              loop(cmp := mbcmp(rx, (m1, v1), ry, (m2, v2)), cmp.ceq(0))(rx, x, ry, y),
              cmp.ceq(0).mux(
                lord.compareNonnull(rx, coerce[lord.T](len1.load()), ry, coerce[lord.T](len2.load()), missingGreatest),
                cmp))
          }

          override def ltNonnull(rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long], missingGreatest: Boolean): Code[Boolean] = {
            val mblt = mb.getCodeOrdering[Boolean](t1.elementType, t2.elementType, CodeOrdering.lt, missingGreatest)
            val mbequiv = mb.getCodeOrdering[Boolean](t1.elementType, t2.elementType, CodeOrdering.equiv, missingGreatest)
            val lt = mb.newLocal[Boolean]
            val lcmp = Code(
              lt := mblt(rx, (m1, v1), ry, (m2, v2)),
              eq := mbequiv(rx, (m1, v1), ry, (m2, v2)))

            Code(lt := false, eq := true,
              loop(lcmp, !lt && eq)(rx, x, ry, y),
              lt || eq && lord.ltNonnull(rx, coerce[lord.T](len1.load()), ry, coerce[lord.T](len2.load()), missingGreatest))
          }

          override def lteqNonnull(rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long], missingGreatest: Boolean): Code[Boolean] = {
            val mblteq = mb.getCodeOrdering[Boolean](t1.elementType, t2.elementType, CodeOrdering.lteq, missingGreatest)
            val mbequiv = mb.getCodeOrdering[Boolean](t1.elementType, t2.elementType, CodeOrdering.equiv, missingGreatest)

            val lteq = mb.newLocal[Boolean]
            val lcmp = Code(
              lteq := mblteq(rx, (m1, v1), ry, (m2, v2)),
              eq := mbequiv(rx, (m1, v1), ry, (m2, v2)))

            Code(lteq := true, eq := true,
              loop(lcmp, eq)(rx, x, ry, y),
              lteq && (!eq || lord.lteqNonnull(rx, coerce[lord.T](len1.load()), ry, coerce[lord.T](len2.load()), missingGreatest)))
          }

          override def gtNonnull(rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long], missingGreatest: Boolean): Code[Boolean] = {
            val mbgt = mb.getCodeOrdering[Boolean](t1.elementType, t2.elementType, CodeOrdering.gt, missingGreatest)
            val mbequiv = mb.getCodeOrdering[Boolean](t1.elementType, t2.elementType, CodeOrdering.equiv, missingGreatest)
            val gt = mb.newLocal[Boolean]
            val lcmp = Code(
              gt := mbgt(rx, (m1, v1), ry, (m2, v2)),
              eq := !gt && mbequiv(rx, (m1, v1), ry, (m2, v2)))

            Code(gt := false,
              eq := true,
              loop(lcmp, eq)(rx, x, ry, y),
              gt || (eq &&
                lord.gtNonnull(rx, coerce[lord.T](len1.load()), ry, coerce[lord.T](len2.load()), missingGreatest)))
          }

          override def gteqNonnull(rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long], missingGreatest: Boolean): Code[Boolean] = {
            val mbgteq = mb.getCodeOrdering[Boolean](t1.elementType, t2.elementType, CodeOrdering.gteq, missingGreatest)
            val mbequiv = mb.getCodeOrdering[Boolean](t1.elementType, t2.elementType, CodeOrdering.equiv, missingGreatest)

            val gteq = mb.newLocal[Boolean]
            val lcmp = Code(
              gteq := mbgteq(rx, (m1, v1), ry, (m2, v2)),
              eq := mbequiv(rx, (m1, v1), ry, (m2, v2)))

            Code(gteq := true,
              eq := true,
              loop(lcmp, eq)(rx, x, ry, y),
              gteq && (!eq || lord.gteqNonnull(rx, coerce[lord.T](len1.load()), ry, coerce[lord.T](len2.load()), missingGreatest)))
          }

          override def equivNonnull(rx: Code[Region], x: Code[Long], ry: Code[Region], y: Code[Long], missingGreatest: Boolean): Code[Boolean] = {
            val mbequiv = mb.getCodeOrdering[Boolean](t1.elementType, t2.elementType, CodeOrdering.equiv, missingGreatest)
            val lcmp = eq := mbequiv(rx, (m1, v1), ry, (m2, v2))
            Code(eq := true,
              loop(lcmp, eq)(rx, x, ry, y),
              eq && lord.equivNonnull(rx, coerce[lord.T](len1.load()), ry, coerce[lord.T](len2.load()), missingGreatest))
          }
        }
      case (ts: TStruct, l: PStruct, r: PStruct) =>
      case (ti: TInterval, l: PInterval, r: PInterval) =>
      case (TCall(_), l: PCall, r: PCall) =>
      case (TLocus(rg, _), l: PLocus, r: PLocus) =>



    }
  }
}

abstract class CodeOrdering {

  outer =>

  type T
  type P = (Code[Boolean], Code[T])

  def compareNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Int]

  def ltNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
    compareNonnull(rx, x, ry, y, missingGreatest) < 0

  def lteqNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
    compareNonnull(rx, x, ry, y, missingGreatest) <= 0

  def gtNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
    compareNonnull(rx, x, ry, y, missingGreatest) > 0

  def gteqNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
    compareNonnull(rx, x, ry, y, missingGreatest) >= 0

  def equivNonnull(rx: Code[Region], x: Code[T], ry: Code[Region], y: Code[T], missingGreatest: Boolean): Code[Boolean] =
    compareNonnull(rx, x, ry, y, missingGreatest).ceq(0)

  def compare(rx: Code[Region], x: P, ry: Code[Region], y: P, missingGreatest: Boolean): Code[Int] = {
    val (xm, xv) = x
    val (ym, yv) = y
    val compMissing = if (missingGreatest) (xm && ym).mux(0, xm.mux(1, -1)) else (xm && ym).mux(0, xm.mux(-1, 1))

    (xm || ym).mux(compMissing, compareNonnull(rx, xv, ry, yv, missingGreatest))
  }

  def lt(rx: Code[Region], x: P, ry: Code[Region], y: P, missingGreatest: Boolean): Code[Boolean] = {
    val (xm, xv) = x
    val (ym, yv) = y
    val compMissing = (xm && ym).mux(false, if (missingGreatest) !xm else xm)

    (xm || ym).mux(compMissing, ltNonnull(rx, xv, ry, yv, missingGreatest))
  }

  def lteq(rx: Code[Region], x: P, ry: Code[Region], y: P, missingGreatest: Boolean): Code[Boolean] = {
    val (xm, xv) = x
    val (ym, yv) = y
    val compMissing = (xm && ym).mux(true, if (missingGreatest) !xm else xm)

    (xm || ym).mux(compMissing, lteqNonnull(rx, xv, ry, yv, missingGreatest))
  }

  def gt(rx: Code[Region], x: P, ry: Code[Region], y: P, missingGreatest: Boolean): Code[Boolean] = {
    val (xm, xv) = x
    val (ym, yv) = y
    val compMissing = (xm && ym).mux(false, if (missingGreatest) xm else !xm)

    (xm || ym).mux(compMissing, gtNonnull(rx, xv, ry, yv, missingGreatest))
  }

  def gteq(rx: Code[Region], x: P, ry: Code[Region], y: P, missingGreatest: Boolean): Code[Boolean] = {
    val (xm, xv) = x
    val (ym, yv) = y
    val compMissing = (xm && ym).mux(true, if (missingGreatest) xm else !xm)

    (xm || ym).mux(compMissing, gteqNonnull(rx, xv, ry, yv, missingGreatest))
  }

  def equiv(rx: Code[Region], x: P, ry: Code[Region], y: P, missingGreatest: Boolean): Code[Boolean] = {
    val (xm, xv) = x
    val (ym, yv) = y

    (xm || ym).mux(xm && ym, equivNonnull(rx, xv, ry, yv, missingGreatest))
  }

  def compare(rx: Code[Region], x: P, ry: Code[Region], y: P): Code[Int] =
    compare(rx, x, ry, y, missingGreatest = true)

  def lt(rx: Code[Region], x: P, ry: Code[Region], y: P): Code[Boolean] =
    lt(rx, x, ry, y, missingGreatest = true)

  def lteq(rx: Code[Region], x: P, ry: Code[Region], y: P): Code[Boolean] =
    lteq(rx, x, ry, y, missingGreatest = true)

  def gt(rx: Code[Region], x: P, ry: Code[Region], y: P): Code[Boolean] =
    gt(rx, x, ry, y, missingGreatest = true)

  def gteq(rx: Code[Region], x: P, ry: Code[Region], y: P): Code[Boolean] =
    gteq(rx, x, ry, y, missingGreatest = true)

  def equiv(rx: Code[Region], x: P, ry: Code[Region], y: P): Code[Boolean] =
    equiv(rx, x, ry, y, missingGreatest = true)
}
