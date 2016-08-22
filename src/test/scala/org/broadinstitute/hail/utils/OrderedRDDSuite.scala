package org.broadinstitute.hail.utils

import org.apache.spark.rdd.{OrderedRDD, RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{OrderedPartitioner, Partition, SparkContext, TaskContext}
import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.check.Arbitrary._
import org.broadinstitute.hail.check.{Gen, Prop, Properties}
import org.broadinstitute.hail.variant.{Locus, Variant}
import org.testng.annotations.Test

class OrderedRDDSuite extends SparkSuite {

  val gen = for (uniqueVariants <- Gen.buildableOf[Set, Variant](Gen.choose(1, 100)
    .map(i => Variant("1", i, "A", "T")))
    .map(set => set.toIndexedSeq);
    toZip <- Gen.buildableOfN[IndexedSeq, String](uniqueVariants.size, arbitrary[String]);
    nPar <- Gen.choose(1, 10)) yield (nPar, uniqueVariants.zip(toZip))

  @Test def testWriteRead() {
    val tmpPartitioner = tmpDir.createTempFile("partitioner")
    val tmpRdd = tmpDir.createTempFile("rdd", ".parquet")

    val p = Prop.forAll(gen) { case (nPar, it) =>
      val rdd = sc.parallelize(it, nPar).toOrderedRDD(_.locus)
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
        OrderedPartitioner.read[Locus, Variant](in, _.locus)
      }

      val orderedRddRB = new OrderedRDD[Locus, Variant, String](rddReadBack, readBackPartitioner)

      orderedRddRB.zipPartitions(rdd) { case (it1, it2) =>
        it1.zip(it2)
      }
        .collect()
        .forall { case (v1, v2) => v1 == v2 }
    }

    p.check()
  }

  object Spec extends Properties("OrderedRDDConstruction") {
    val v = for (pos <- Gen.choose(1, 100);
      alt <- genDNAString.filter(_ != "A")) yield Variant("16", pos, "A", alt)

    val g = for (uniqueVariants <- Gen.buildableOf[Set, Variant](v).map(set => set.toIndexedSeq);
      toZip <- Gen.buildableOfN[IndexedSeq, String](uniqueVariants.size, arbitrary[String]);
      nPar <- Gen.choose(1, 10)) yield (nPar, uniqueVariants.zip(toZip))
    val random = for ((n, v) <- g;
      shuffled <- Gen.shuffle(v)) yield (n, shuffled)

    val locusSorted = for ((n, v) <- g;
      locusSorted <- Gen.const(v.sortBy(_._1.locus))) yield (n, locusSorted)

    val sorted = for ((n, v) <- g;
      sorted <- Gen.const(v.sortBy(_._1))) yield (n, sorted)

    def check(rdd: OrderedRDD[Locus, Variant, String]): Boolean = {
      val p = rdd.orderedPartitioner
      val partitionSummaries = rdd.mapPartitionsWithIndex { case (partitionIndex, iter) =>
        val a = iter.toArray
        val keys = a.map(_._1)
        val sorted = keys.isSorted
        val correctPartitioning = keys.forall(k => p.getPartition(k) == partitionIndex)
        Iterator((partitionIndex, sorted, correctPartitioning, if (a.nonEmpty) Some((a.head._1, a.last._1)) else None))
      }.collect().sortBy(_._1)

      partitionSummaries.flatMap(_._4.map(_._1)).headOption match {
        case Some(first) =>
          val sortedWithin = partitionSummaries.forall(_._2)
          val partitionedCorrectly = partitionSummaries.forall(_._3)
          val sortedBetween = partitionSummaries.flatMap(_._4)
            .tail
            .foldLeft((true, first)) { case ((b, last), (start, end)) =>
              (b && start > last, end)
            }._1
          sortedWithin && partitionedCorrectly && sortedBetween
        case None => true
      }
    }

    property("randomlyOrdered") = Prop.forAll(random) { case (nPar, s) =>
      check(sc.parallelize(s, nPar).toOrderedRDD(_.locus))
    }

    property("locusSorted") = Prop.forAll(locusSorted) { case (nPar, s) =>
      check(sc.parallelize(s, nPar).toOrderedRDD(_.locus))
    }

    property("fullySorted") = Prop.forAll(sorted) { case (nPar, s) =>
      check(sc.parallelize(s, nPar).toOrderedRDD(_.locus))
    }
  }


  @Test def testConstruction() {
    Spec.check()
  }
}

