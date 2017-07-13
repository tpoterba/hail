package is.hail.utils

import scala.collection.mutable

class BytePacker {
  val slots = new mutable.TreeSet[(Int, Int)]

  def insertSpace(start: Int, size: Int) {
    slots += start -> size
  }

  def getSpace(size: Int, alignment: Int): Option[Int] = {
    slots.foreach { x =>
      val start = x._1
      val spaceSize = x._2
      var i = 0
      while (i + size <= spaceSize) {
        if ((start + i) % alignment == 0) {
          // space found
          slots -= x
          if (i > 0) {
            slots += start -> i
          }
          val endGap = spaceSize - size - i
          if (endGap > 0) {
            slots += (start + i + size) -> endGap
          }
          return Some(start + i)
        }
        i += 1
      }
    }
    None
  }
}