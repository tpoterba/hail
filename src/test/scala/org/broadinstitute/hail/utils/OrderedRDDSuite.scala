package org.broadinstitute.hail.utils

import org.apache.spark.{OrderedPartitioner, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.{OrderedRDD, RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.check.Arbitrary._
import org.broadinstitute.hail.check.{Gen, Prop, Properties}
import org.broadinstitute.hail.variant.{Locus, Variant}
import org.testng.annotations.Test

class OrderedRDDSuite extends SparkSuite {

  val gen = for (uniqueVariants <- Gen.buildableOf[Set[Variant], Variant](Gen.choose(1, 100)
    .map(i => Variant("1", i, "A", "T")))
    .map(set => set.toIndexedSeq);
                 toZip <- Gen.buildableOfN[IndexedSeq[String], String](uniqueVariants.size, arbitrary[String]);
                 nPar <- Gen.choose(1, 10)) yield (nPar, uniqueVariants.zip(toZip))

  @Test def testJoin() {

    val p = Prop.forAll(gen, gen) { case ((nPar1, it1), (nPar2, it2)) =>
      val m2 = it2.toMap

      val rdd1 = sc.parallelize(it1, nPar1).cache()
      val rdd2 = sc.parallelize(it2, nPar2).cache()

      val join: IndexedSeq[(Variant, (String, Option[String]))] = rdd1.orderedLeftJoinDistinct[Locus, String](rdd2).collect().toIndexedSeq

      val check1 = it1.toMap == join.map { case (k, (v1, _)) => (k, v1) }.toMap
      val check2 = join.forall { case (k, (_, v2)) => v2 == m2.get(k) }
      val check3 = rdd1.leftOuterJoinDistinct(rdd2).collect().toMap == join.toMap

      println(check1, check2, check3)

      check1 && check2 && check3
    }

    p.check(size = 100)
  }

  @Test def testWriteRead() {
    val tmpPartitioner = tmpDir.createTempFile("partitioner")
    val tmpRdd = tmpDir.createTempFile("rdd", ".parquet")

    val p = Prop.forAll(gen) { case (nPar, it) =>
      val rdd = OrderedRDD(sc.parallelize(it, nPar))
      val schema = StructType(Array(
        StructField("variant", Variant.schema, nullable = false),
        StructField("str", StringType, nullable = false)))
      hadoopDelete(tmpRdd, hadoopConf, recursive = true)
      val df = sqlContext.createDataFrame(rdd.map { case (v, s) => Row.fromSeq(Seq(v.toRow, s)) }, schema)
        .write.parquet(tmpRdd)

      writeObjectFile(tmpPartitioner, hadoopConf) { out =>
        rdd.partitioner.get.asInstanceOf[OrderedPartitioner[Variant, String]].write(out)
      }

      val status = hadoopFileStatus(tmpPartitioner, hadoopConf)

      val rddReadBack = sqlContext.readPartitioned.parquet(tmpRdd)
        .rdd
        .map(r => (Variant.fromRow(r.getAs[Row](0)), r.getAs[String](1)))

      val readBackPartitioner = readObjectFile(tmpPartitioner, hadoopConf) { in =>
        OrderedPartitioner.read[Locus, Variant](in)
      }

      val orderedRddRB = new OrderedRDD[Locus, Variant, String](rddReadBack, readBackPartitioner)

      orderedRddRB.zipPartitions(rdd) { case (it1, it2) =>
        it1.zip(it2)
      }
        .collect()
        .forall { case (v1, v2) => v1 == v2 }
    }

    p.check(size = 100)
  }

  object Spec extends Properties("OrderedRDDConstruction") {
    val v = for (pos <- Gen.choose(1, 100);
                 alt <- genDNAString.filter(_ != "A")) yield Variant("16", pos, "A", alt)

    val g = for (uniqueVariants <- Gen.buildableOf[Set[Variant], Variant](v).map(set => set.toIndexedSeq);
                 toZip <- Gen.buildableOfN[IndexedSeq[String], String](uniqueVariants.size, arbitrary[String]);
                 nPar <- Gen.choose(1, 10)) yield (nPar, uniqueVariants.zip(toZip))
    val random = for ((n, v) <- g;
                      shuffled <- Gen.shuffle(v)) yield (n, shuffled)

    val locusSorted = for ((n, v) <- g;
                           locusSorted <- Gen.const(v.sortBy(_._1.locus))) yield (n, locusSorted)

    val sorted = for ((n, v) <- g;
                      sorted <- Gen.const(v.sortBy(_._1))) yield (n, sorted)

    def check(rdd: RDD[(Variant, String)]): Boolean = {
      val partitionSummaries = rdd.mapPartitionsWithIndex { case (partitionIndex, iter) =>
        val a = iter.toArray
        val sorted = a.map(_._1).isSorted
        Iterator((partitionIndex, sorted, if (a.nonEmpty) Some((a.head._1, a.last._1)) else None))
      }.collect().sortBy(_._1)

      partitionSummaries.flatMap(_._3.map(_._1)).headOption match {
        case Some(first) =>
          val sortedWithin = partitionSummaries.forall(_._2)
          val sortedBetween = partitionSummaries.flatMap(_._3)
            .tail
            .foldLeft((true, first)) { case ((b, last), (start, end)) =>
              (b && start > last, end)
            }._1
          sortedWithin && sortedBetween
        case None => true
      }
    }

    property("randomlyOrdered") = Prop.forAll(random) { case (nPar, s) =>
      check(OrderedRDD[Locus, Variant, String](sc.parallelize(s, nPar)))
    }

    property("locusSorted") = Prop.forAll(locusSorted) { case (nPar, s) =>
      check(OrderedRDD[Locus, Variant, String](sc.parallelize(s, nPar)))
    }

    property("fullySorted") = Prop.forAll(sorted) { case (nPar, s) =>
      check(OrderedRDD[Locus, Variant, String](sc.parallelize(s, nPar)))
    }
  }


  @Test def testConstruction() {
    Spec.check(size = 100)
    Spec.check(size = 10) // produce more empty sets and partitions
  }
}

case class SimplePartition(index: Int) extends Partition

abstract class TestRDD(sc: SparkContext) extends RDD[(Variant, Unit)](sc, Seq.empty) {
  override def getPartitions: Array[Partition] = Array(SimplePartition(0), SimplePartition(1), SimplePartition(2))
}

class Unsorted(sc: SparkContext) extends TestRDD(sc) {
  override def compute(split: Partition, context: TaskContext): Iterator[(Variant, Unit)] = {
    val i = split.index
    if (i == 0)
      Iterator(
        Variant("1", 1, "A", "T"),
        Variant("1", 20, "A", "T"),
        Variant("1", 2, "A", "T")
      ).map(v => (v, ()))
    else if (i == 1)
      Iterator(
        Variant("1", 3, "A", "T"),
        Variant("1", 5, "A", "T"),
        Variant("1", 6, "A", "T")
      ).map(v => (v, ()))
    else
      Iterator(
        Variant("1", 200, "A", "T"),
        Variant("1", 4, "A", "T"),
        Variant("1", 11, "A", "T")
      ).map(v => (v, ()))
  }
}

class Sorted(sc: SparkContext) extends TestRDD(sc) {
  override def compute(split: Partition, context: TaskContext): Iterator[(Variant, Unit)] = {
    val i = split.index
    if (i == 0)
      Iterator(
        Variant("1", 1, "A", "T"),
        Variant("1", 2, "A", "T"),
        Variant("1", 3, "A", "T")
      ).map(v => (v, ()))
    else if (i == 1)
      Iterator(
        Variant("1", 4, "A", "T"),
        Variant("1", 5, "A", "T"),
        Variant("1", 6, "A", "T")
      ).map(v => (v, ()))
    else
      Iterator(
        Variant("1", 11, "A", "T"),
        Variant("1", 20, "A", "T"),
        Variant("1", 200, "A", "T")
      ).map(v => (v, ()))
  }
}
