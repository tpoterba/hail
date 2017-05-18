package is.hail.annotations

import is.hail.expr._
import sun.misc.Unsafe

object UnsafeAnnotations {
//  /**
//    * Thoughts about design
//    * Array should store n elems, total size?
//    * Struct should store missing bits, offset of each elem
//    * Set == Array
//    * Dict is array of keys, array of values
//    *
//    *
//    */
//
//  def write(u: Unsafe, t: Type, a: Annotation): Long = {
//    t match {
//      case TInt =>
//      case TLong =>
//      case TFloat =>
//      case TDouble =>
//      case TBoolean =>
//    }
//  }
//
//  def read(u: Unsafe, mem: Long, t: Type): Annotation = {
//
//
//  }
//
//  def unsafeIndexedSeq
//
//  def
}

//class UniformUnsafeArray[T](u: Unsafe, val length: Int, elemSize: Int, start: Long, t: Type) extends IndexedSeq[T] {
//
//}
//
//class UnsafeIndexedSeq[T](u: Unsafe, loc: Long, t: Type)(implicit hr: HailRep[T]) extends IndexedSeq[T] {
//  val length = u.getInt(loc)
//  val uniform = u.getByte(loc + 4)
//
//
//  def apply(idx: Int): T = ???
//}