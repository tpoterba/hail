package is.hail.expr.ir

import is.hail.asm4s._
import is.hail.types.physical.{PCode, PType, PValue, typeToTypeInfo}
import is.hail.types.virtual.Type
import is.hail.utils.BoxedArrayBuilder

import scala.reflect.ClassTag

class StagedArrayBuilder(val elt: PType, mb: EmitMethodBuilder[_], len: Code[Int]) {

  val ti: TypeInfo[_] = typeToTypeInfo(elt)

  val ref: Value[Any] = coerce[Any](ti match {
    case BooleanInfo => mb.genLazyFieldThisRef[BooleanMissingArrayBuilder](Code.newInstance[BooleanMissingArrayBuilder, Int](len), "zab")
    case IntInfo => mb.genLazyFieldThisRef[IntMissingArrayBuilder](Code.newInstance[IntMissingArrayBuilder, Int](len), "iab")
    case LongInfo => mb.genLazyFieldThisRef[LongMissingArrayBuilder](Code.newInstance[LongMissingArrayBuilder, Int](len), "jab")
    case FloatInfo => mb.genLazyFieldThisRef[FloatMissingArrayBuilder](Code.newInstance[FloatMissingArrayBuilder, Int](len), "fab")
    case DoubleInfo => mb.genLazyFieldThisRef[DoubleArrayBuilder](Code.newInstance[DoubleArrayBuilder, Int](len), "dab")
    case ti => throw new RuntimeException(s"unsupported typeinfo found: $ti")
  })

  def add(x: Code[_]): Code[Unit] = ti match {
    case BooleanInfo => coerce[BooleanMissingArrayBuilder](ref).invoke[Boolean, Unit]("add", coerce[Boolean](x))
    case IntInfo => coerce[IntMissingArrayBuilder](ref).invoke[Int, Unit]("add", coerce[Int](x))
    case LongInfo => coerce[LongMissingArrayBuilder](ref).invoke[Long, Unit]("add", coerce[Long](x))
    case FloatInfo => coerce[FloatMissingArrayBuilder](ref).invoke[Float, Unit]("add", coerce[Float](x))
    case DoubleInfo => coerce[DoubleArrayBuilder](ref).invoke[Double, Unit]("add", coerce[Double](x))
  }

  def apply(i: Code[Int]): Code[_] = ti match {
    case BooleanInfo => coerce[BooleanMissingArrayBuilder](ref).invoke[Int, Boolean]("apply", i)
    case IntInfo => coerce[IntMissingArrayBuilder](ref).invoke[Int, Int]("apply", i)
    case LongInfo => coerce[LongMissingArrayBuilder](ref).invoke[Int, Long]("apply", i)
    case FloatInfo => coerce[FloatMissingArrayBuilder](ref).invoke[Int, Float]("apply", i)
    case DoubleInfo => coerce[DoubleArrayBuilder](ref).invoke[Int, Double]("apply", i)
  }

  def update(i: Code[Int], x: Code[_]): Code[Unit] = ti match {
    case BooleanInfo => coerce[BooleanMissingArrayBuilder](ref).invoke[Int, Boolean, Unit]("update", i, coerce[Boolean](x))
    case IntInfo => coerce[IntMissingArrayBuilder](ref).invoke[Int, Int, Unit]("update", i, coerce[Int](x))
    case LongInfo => coerce[LongMissingArrayBuilder](ref).invoke[Int, Long, Unit]("update", i, coerce[Long](x))
    case FloatInfo => coerce[FloatMissingArrayBuilder](ref).invoke[Int, Float, Unit]("update", i, coerce[Float](x))
    case DoubleInfo => coerce[DoubleArrayBuilder](ref).invoke[Int, Double, Unit]("update", i, coerce[Double](x))
  }

  def sort(compare: Code[AsmFunction2[_, _, _]]): Code[Unit] = {
    ti match {
      case BooleanInfo =>
        type F = AsmFunction2[Boolean, Boolean, Boolean]
        coerce[BooleanMissingArrayBuilder](ref).invoke[F, Unit]("sort", coerce[F](compare))
      case IntInfo =>
        type F = AsmFunction2[Int, Int, Boolean]
        coerce[IntMissingArrayBuilder](ref).invoke[F, Unit]("sort", coerce[F](compare))
      case LongInfo =>
        type F = AsmFunction2[Long, Long, Boolean]
        coerce[LongMissingArrayBuilder](ref).invoke[F, Unit]("sort", coerce[F](compare))
      case FloatInfo =>
        type F = AsmFunction2[Float, Float, Boolean]
        coerce[FloatMissingArrayBuilder](ref).invoke[F, Unit]("sort", coerce[F](compare))
      case DoubleInfo =>
        type F = AsmFunction2[Double, Double, Boolean]
        coerce[DoubleArrayBuilder](ref).invoke[F, Unit]("sort", coerce[F](compare))
    }
  }

  def addMissing(): Code[Unit] =
    coerce[MissingArrayBuilder](ref).invoke[Unit]("addMissing")

  def isMissing(i: Code[Int]): Code[Boolean] =
    coerce[MissingArrayBuilder](ref).invoke[Int, Boolean]("isMissing", i)

  def setMissing(i: Code[Int], m: Code[Boolean]): Code[Unit] =
    coerce[MissingArrayBuilder](ref).invoke[Int, Boolean, Unit]("setMissing", i, m)

  def size: Code[Int] = coerce[MissingArrayBuilder](ref).invoke[Int]("size")

  def setSize(n: Code[Int]): Code[Unit] = coerce[MissingArrayBuilder](ref).invoke[Int, Unit]("setSize", n)

  def ensureCapacity(n: Code[Int]): Code[Unit] = coerce[MissingArrayBuilder](ref).invoke[Int, Unit]("ensureCapacity", n)

  def clear: Code[Unit] = coerce[MissingArrayBuilder](ref).invoke[Unit]("clear")

  def applyEV(mb: EmitMethodBuilder[_], i: Code[Int]): EmitValue =
    new EmitValue {
      def pt: PType = elt

      def get(cb: EmitCodeBuilder): PValue = load.toI(cb).handle(
        cb,
        s"Can't convert missing EmitValue of type ${pt} to PValue.").memoize(cb, "sab_apply_EV_get_i")

      def load: EmitCode = {
        val t = mb.newLocal[Int]("sab_applyEV_load_i")
        EmitCode(t := i, isMissing(t), PCode(elt, apply(t)))
      }
    }
}

sealed abstract class MissingArrayBuilder(initialCapacity: Int) {

  var missing: Array[Boolean] = new Array[Boolean](initialCapacity)
  var size_ : Int = 0

  def size: Int = size_

  def isMissing(i: Int): Boolean = {
    require(i >= 0 && i < size)
    missing(i)
  }

  def ensureCapacity(n: Int): Unit

  def setMissing(i: Int, m: Boolean): Unit = {
    require(i >= 0 && i < size, i)
    missing(i) = m
  }

  def addMissing() {
    ensureCapacity(size_ + 1)
    missing(size_) = true
    size_ += 1
  }

  def setSize(n: Int) {
    require(n >= 0 && n <= size)
    size_ = n
  }

  def clear() {  size_ = 0 }
}

class IntMissingArrayBuilder(initialCapacity: Int) extends MissingArrayBuilder(initialCapacity) {
  private var b: Array[Int] = new Array[Int](initialCapacity)

  def apply(i: Int): Int = {
    require(i >= 0 && i < size)
    b(i)
  }

  def ensureCapacity(n: Int): Unit = {
    if (b.length < n) {
      val newCapacity = (b.length * 2).max(n)
      val newb = new Array[Int](newCapacity)
      Array.copy(b, 0, newb, 0, size_)
      b = newb
      val newmissing = new Array[Boolean](newCapacity)
      Array.copy(missing, 0, newmissing, 0, size_)
      missing = newmissing
    }
  }

  def add(x: Int): Unit = {
    ensureCapacity(size_ + 1)
    b(size_) = x
    missing(size_) = false
    size_ += 1
  }

  def update(i: Int, x: Int): Unit = {
    require(i >= 0 && i < size)
    b(i) = x
    missing(i) = false
  }

  def sort(ordering: AsmFunction2[Int, Int, Boolean]): Unit = {
    var newend = 0
    var i = 0
    while (i < size) {
      if (!isMissing(i)) {
        if (newend != i) {
          update(newend, b(i))
        }
        newend += 1
      }
      i += 1
    }
    i = newend
    while (i < size) {
      setMissing(i, true)
      i += 1
    }
    val newb = b.take(newend).sortWith(ordering(_, _))
    i = 0
    while (i < newend) {
      update(i, newb(i))
      i += 1
    }
  }
}

class LongMissingArrayBuilder(initialCapacity: Int) extends MissingArrayBuilder(initialCapacity) {
  private var b: Array[Long] = new Array[Long](initialCapacity)

  def apply(i: Int): Long = {
    require(i >= 0 && i < size)
    b(i)
  }

  def ensureCapacity(n: Int): Unit = {
    if (b.length < n) {
      val newCapacity = (b.length * 2).max(n)
      val newb = new Array[Long](newCapacity)
      Array.copy(b, 0, newb, 0, size_)
      b = newb
      val newmissing = new Array[Boolean](newCapacity)
      Array.copy(missing, 0, newmissing, 0, size_)
      missing = newmissing
    }
  }

  def add(x: Long): Unit = {
    ensureCapacity(size_ + 1)
    b(size_) = x
    missing(size_) = false
    size_ += 1
  }

  def update(i: Int, x: Long): Unit = {
    require(i >= 0 && i < size)
    b(i) = x
    missing(i) = false
  }

  def sort(ordering: AsmFunction2[Long, Long, Boolean]): Unit = {
    var newend = 0
    var i = 0
    while (i < size) {
      if (!isMissing(i)) {
        if (newend != i) {
          update(newend, b(i))
        }
        newend += 1
      }
      i += 1
    }
    i = newend
    while (i < size) {
      setMissing(i, true)
      i += 1
    }
    val newb = b.take(newend).sortWith(ordering(_, _))
    i = 0
    while (i < newend) {
      update(i, newb(i))
      i += 1
    }
  }
}

class FloatMissingArrayBuilder(initialCapacity: Int) extends MissingArrayBuilder(initialCapacity) {
  private var b: Array[Float] = new Array[Float](initialCapacity)

  def apply(i: Int): Float = {
    require(i >= 0 && i < size)
    b(i)
  }

  def ensureCapacity(n: Int): Unit = {
    if (b.length < n) {
      val newCapacity = (b.length * 2).max(n)
      val newb = new Array[Float](newCapacity)
      Array.copy(b, 0, newb, 0, size_)
      b = newb
      val newmissing = new Array[Boolean](newCapacity)
      Array.copy(missing, 0, newmissing, 0, size_)
      missing = newmissing
    }
  }

  def add(x: Float): Unit = {
    ensureCapacity(size_ + 1)
    b(size_) = x
    missing(size_) = false
    size_ += 1
  }

  def update(i: Int, x: Float): Unit = {
    require(i >= 0 && i < size)
    b(i) = x
    missing(i) = false
  }

  def sort(ordering: AsmFunction2[Float, Float, Boolean]): Unit = {
    var newend = 0
    var i = 0
    while (i < size) {
      if (!isMissing(i)) {
        if (newend != i) {
          update(newend, b(i))
        }
        newend += 1
      }
      i += 1
    }
    i = newend
    while (i < size) {
      setMissing(i, true)
      i += 1
    }
    val newb = b.take(newend).sortWith(ordering(_, _))
    i = 0
    while (i < newend) {
      update(i, newb(i))
      i += 1
    }
  }
}

class DoubleArrayBuilder(initialCapacity: Int) extends MissingArrayBuilder(initialCapacity) {
  private var b: Array[Double] = new Array[Double](initialCapacity)

  def apply(i: Int): Double = {
    require(i >= 0 && i < size)
    b(i)
  }

  def ensureCapacity(n: Int): Unit = {
    if (b.length < n) {
      val newCapacity = (b.length * 2).max(n)
      val newb = new Array[Double](newCapacity)
      Array.copy(b, 0, newb, 0, size_)
      b = newb
      val newmissing = new Array[Boolean](newCapacity)
      Array.copy(missing, 0, newmissing, 0, size_)
      missing = newmissing
    }
  }

  def add(x: Double): Unit = {
    ensureCapacity(size_ + 1)
    b(size_) = x
    missing(size_) = false
    size_ += 1
  }

  def update(i: Int, x: Double): Unit = {
    require(i >= 0 && i < size)
    b(i) = x
    missing(i) = false
  }

  def sort(ordering: AsmFunction2[Double, Double, Boolean]): Unit = {
    var newend = 0
    var i = 0
    while (i < size) {
      if (!isMissing(i)) {
        if (newend != i) {
          update(newend, b(i))
        }
        newend += 1
      }
      i += 1
    }
    i = newend
    while (i < size) {
      setMissing(i, true)
      i += 1
    }
    val newb = b.take(newend).sortWith(ordering(_, _))
    i = 0
    while (i < newend) {
      update(i, newb(i))
      i += 1
    }
  }
}

class BooleanMissingArrayBuilder(initialCapacity: Int) extends MissingArrayBuilder(initialCapacity) {
  private var b: Array[Boolean] = new Array[Boolean](initialCapacity)

  def apply(i: Int): Boolean = {
    require(i >= 0 && i < size)
    b(i)
  }

  def ensureCapacity(n: Int): Unit = {
    if (b.length < n) {
      val newCapacity = (b.length * 2).max(n)
      val newb = new Array[Boolean](newCapacity)
      Array.copy(b, 0, newb, 0, size_)
      b = newb
      val newmissing = new Array[Boolean](newCapacity)
      Array.copy(missing, 0, newmissing, 0, size_)
      missing = newmissing
    }
  }

  def add(x: Boolean): Unit = {
    ensureCapacity(size_ + 1)
    b(size_) = x
    missing(size_) = false
    size_ += 1
  }

  def update(i: Int, x: Boolean): Unit = {
    require(i >= 0 && i < size)
    b(i) = x
    missing(i) = false
  }

  def sort(ordering: AsmFunction2[Boolean, Boolean, Boolean]): Unit = {
    var newend = 0
    var i = 0
    while (i < size) {
      if (!isMissing(i)) {
        if (newend != i) {
          update(newend, b(i))
        }
        newend += 1
      }
      i += 1
    }
    i = newend
    while (i < size) {
      setMissing(i, true)
      i += 1
    }
    val newb = b.take(newend).sortWith(ordering(_, _))
    i = 0
    while (i < newend) {
      update(i, newb(i))
      i += 1
    }
  }
}

class ByteArrayArrayBuilder(initialCapacity: Int) {

  var size_ : Int = 0
  private var b: Array[Array[Byte]] = new Array[Array[Byte]](initialCapacity)

  def size: Int = size_

  def setSize(n: Int) {
    require(n >= 0 && n <= size)
    size_ = n
  }

  def apply(i: Int): Array[Byte] = {
    require(i >= 0 && i < size)
    b(i)
  }

  def ensureCapacity(n: Int): Unit = {
    if (b.length < n) {
      val newCapacity = (b.length * 2).max(n)
      val newb = new Array[Array[Byte]](newCapacity)
      Array.copy(b, 0, newb, 0, size_)
      b = newb
    }
  }

  def add(x: Array[Byte]): Unit = {
    ensureCapacity(size_ + 1)
    b(size_) = x
    size_ += 1
  }

  def update(i: Int, x: Array[Byte]): Unit = {
    require(i >= 0 && i < size)
    b(i) = x
  }

  def clear() {  size_ = 0 }

  def result(): Array[Array[Byte]] = b.slice(0, size_)
}


class LongArrayBuilder(initialCapacity: Int) {

  var size_ : Int = 0
  var b: Array[Long] = new Array[Long](initialCapacity)

  def size: Int = size_

  def setSize(n: Int) {
    require(n >= 0 && n <= size)
    size_ = n
  }

  def apply(i: Int): Long = {
    require(i >= 0 && i < size)
    b(i)
  }

  def ensureCapacity(n: Int): Unit = {
    if (b.length < n) {
      val newCapacity = (b.length * 2).max(n)
      val newb = new Array[Long](newCapacity)
      Array.copy(b, 0, newb, 0, size_)
      b = newb
    }
  }

  def add(x: Long): Unit = {
    ensureCapacity(size_ + 1)
    b(size_) = x
    size_ += 1
  }

  def update(i: Int, x: Long): Unit = {
    require(i >= 0 && i < size)
    b(i) = x
  }

  def clear() {  size_ = 0 }

  def result(): Array[Long] = b.slice(0, size_)

  def clearAndResize(): Unit = {
    size_ = 0
    if (b.length > initialCapacity)
      b = new Array[Long](initialCapacity)
  }
  def appendFrom(ab2: LongArrayBuilder): Unit = {
    ensureCapacity(size_ + ab2.size_)
    System.arraycopy(ab2.b, 0, b, size_, ab2.size_)
    size_ = size_ + ab2.size_
  }

  def pop(): Long = {
    size_ -= 1
    b(size)
  }
}

class IntArrayBuilder(initialCapacity: Int) {

  var size_ : Int = 0
  var b: Array[Int] = new Array[Int](initialCapacity)

  def size: Int = size_

  def setSize(n: Int) {
    require(n >= 0 && n <= size)
    size_ = n
  }

  def apply(i: Int): Int = {
    require(i >= 0 && i < size)
    b(i)
  }

  def ensureCapacity(n: Int): Unit = {
    if (b.length < n) {
      val newCapacity = (b.length * 2).max(n)
      val newb = new Array[Int](newCapacity)
      Array.copy(b, 0, newb, 0, size_)
      b = newb
    }
  }

  def add(x: Int): Unit = {
    ensureCapacity(size_ + 1)
    b(size_) = x
    size_ += 1
  }

  def update(i: Int, x: Int): Unit = {
    require(i >= 0 && i < size)
    b(i) = x
  }

  def clear() {  size_ = 0 }

  def result(): Array[Int] = b.slice(0, size_)

  def clearAndResize(): Unit = {
    size_ = 0
    if (b.length > initialCapacity)
      b = new Array[Int](initialCapacity)
  }
  def appendFrom(ab2: IntArrayBuilder): Unit = {
    ensureCapacity(size_ + ab2.size_)
    System.arraycopy(ab2.b, 0, b, size_, ab2.size_)
    size_ = size_ + ab2.size_
  }

  def pop(): Int = {
    size_ -= 1
    b(size)
  }
}
