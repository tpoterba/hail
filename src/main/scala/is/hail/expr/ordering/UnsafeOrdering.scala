package is.hail.expr.ordering

import is.hail.annotations.{Region, RegionValue}
import is.hail.expr.types._
import is.hail.expr.types.physical._

abstract class UnsafeOrdering extends Ordering[RegionValue] with Serializable {
  def compare(r1: Region, o1: Long, r2: Region, o2: Long): Int

  def compare(rv1: RegionValue, rv2: RegionValue): Int =
    compare(rv1.region, rv1.offset, rv2.region, rv2.offset)

  def compare(rv1: RegionValue, r2: Region, o2: Long): Int =
    compare(rv1.region, rv1.offset, r2, o2)

  def compare(r1: Region, o1: Long, rv2: RegionValue): Int =
    compare(r1, o1, rv2.region, rv2.offset)
}

object UnsafeOrdering {
  def apply(virtualType: Type, left: PType, right: PType, missingGreatest: Boolean): UnsafeOrdering = {
    assert(virtualType == left.virtualType)
    assert(virtualType == right.virtualType)
    ((virtualType, left, right): @unchecked) match {
      case (TInt32(_), left: PInt32, right: PInt32) => new UnsafeOrdering {
        def compare(r1: Region, o1: Long, r2: Region, o2: Long): Int = {
          Integer.compare(left.load(r1, o1), right.load(r2, o2))
        }
      }
      case (TInt64(_), left: PInt64, right: PInt64) => new UnsafeOrdering {
        def compare(r1: Region, o1: Long, r2: Region, o2: Long): Int = {
          java.lang.Long.compare(left.load(r1, o1), right.load(r2, o2))
        }
      }
      case (TFloat32(_), left: PFloat32, right: PFloat32) => new UnsafeOrdering {
        def compare(r1: Region, o1: Long, r2: Region, o2: Long): Int = {
          java.lang.Float.compare(left.load(r1, o1), right.load(r2, o2))
        }
      }
      case (TFloat64(_), left: PFloat64, right: PFloat64) => new UnsafeOrdering {
        def compare(r1: Region, o1: Long, r2: Region, o2: Long): Int = {
          java.lang.Double.compare(left.load(r1, o1), right.load(r2, o2))
        }
      }
      case (TBoolean(_), left: PBoolean, right: PBoolean) => new UnsafeOrdering {
        def compare(r1: Region, o1: Long, r2: Region, o2: Long): Int = {
          java.lang.Boolean.compare(left.load(r1, o1), right.load(r2, o2))
        }
      }
      case (TCall(_), left: PInt32, right: PInt32) => new UnsafeOrdering {
        def compare(r1: Region, o1: Long, r2: Region, o2: Long): Int = {
          Integer.compare(left.load(r1, o1), right.load(r2, o2))
        }
      }
      case (t: TBaseStruct, left: PStruct, right: PStruct) =>
        val fieldOrderings: Array[UnsafeOrdering] = (t.types, left.types, right.types)
          .zipped
          .map { case (virt, pLeft, pRight) => UnsafeOrdering(virt, pLeft, pRight, missingGreatest) }

        new UnsafeOrdering {
          def compare(r1: Region, o1: Long, r2: Region, o2: Long): Int = {
            var i = 0
            while (i < t.types.length) {
              val leftDefined = left.isFieldDefined(r1, o1, i)
              val rightDefined = right.isFieldDefined(r2, o2, i)

              if (leftDefined && rightDefined) {
                val c = fieldOrderings(i).compare(r1, left.loadField(r1, o1, i), r2, right.loadField(r2, o2, i))
                if (c != 0)
                  return c
              } else if (leftDefined != rightDefined) {
                val c = if (leftDefined) -1 else 1
                if (missingGreatest)
                  return c
                else
                  return -c
              }
              i += 1
            }
            0
          }
        }
      case (t: TContainer, left: PArray, right: PArray) =>
        val eltOrd = UnsafeOrdering(t.elementType, left.elementType, right.elementType, missingGreatest)

        new UnsafeOrdering {
          override def compare(r1: Region, o1: Long, r2: Region, o2: Long): Int = {
            val length1 = left.loadLength(r1, o1)
            val length2 = right.loadLength(r2, o2)

            var i = 0
            while (i < math.min(length1, length2)) {
              val leftDefined = left.isElementDefined(r1, o1, i)
              val rightDefined = right.isElementDefined(r2, o2, i)

              if (leftDefined && rightDefined) {
                val eOff1 = left.loadElement(r1, o1, length1, i)
                val eOff2 = right.loadElement(r2, o2, length2, i)
                val c = eltOrd.compare(r1, eOff1, r2, eOff2)
                if (c != 0)
                  return c
              } else if (leftDefined != rightDefined) {
                val c = if (leftDefined) -1 else 1
                if (missingGreatest)
                  return c
                else
                  return -c
              }
              i += 1
            }
            Integer.compare(length1, length2)
          }
        }

      case (t: TInterval, left: PStruct, right: PStruct) =>
        val pOrd = t.pointType.unsafeOrdering(missingGreatest)
        new UnsafeOrdering {
          def compare(r1: Region, o1: Long, r2: Region, o2: Long): Int = {
            val sdef1 = startDefined(r1, o1)
            if (sdef1 == startDefined(r2, o2)) {
              val cmp = pOrd.compare(r1, loadStart(r1, o1), r2, loadStart(r2, o2))
              if (cmp == 0) {
                val includesS1 = includesStart(r1, o1)
                if (includesS1 == includesStart(r2, o2)) {
                  val edef1 = endDefined(r1, o1)
                  if (edef1 == endDefined(r2, o2)) {
                    val cmp = pOrd.compare(r1, loadEnd(r1, o1), r2, loadEnd(r2, o2))
                    if (cmp == 0) {
                      val includesE1 = includesEnd(r1, o1)
                      if (includesE1 == includesEnd(r2, o2)) {
                        0
                      } else if (includesE1) 1 else -1
                    } else cmp
                  } else if (edef1 == missingGreatest) -1 else 1
                } else if (includesS1) -1 else 1
              } else cmp
            } else {
              if (sdef1 == missingGreatest) -1 else 1
            }
          }
        }

      case (t: TLocus, left: PStruct, right: PStruct) =>
        val rg = t.rg
        val left1 = left.types(0).asInstanceOf[PString]
        val right1 = right.types(0).asInstanceOf[PString]
        val posOrd = UnsafeOrdering(TInt32(), left.types(1), right.types(1), missingGreatest)

        new UnsafeOrdering {
          def compare(r1: Region, o1: Long, r2: Region, o2: Long): Int = {
            val cOff1 = left.loadField(r1, o1, 0)
            val cOff2 = right.loadField(r2, o2, 0)

            val leftContig = left1.loadString(r1, cOff1)
            val rightContig = right1.loadString(r2, cOff2)

            val c = rg.compare(leftContig, rightContig)
            if (c != 0)
              return c

            posOrd.compare(r1, left.loadField(r1, o1, 1), r2, right.loadField(r2, o2, 1))
          }
        }
    }
  }
}
