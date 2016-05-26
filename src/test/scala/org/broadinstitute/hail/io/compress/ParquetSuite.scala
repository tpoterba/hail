package org.broadinstitute.hail.io.compress

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.Utils._
import org.testng.annotations.Test

case class ParquetStore(i: Int, j: Int)

class ParquetSuite extends SparkSuite {


  @Test def testParquet() {


    val SIZE = 1000

    val data = (0 until SIZE).flatMap(i => (0 until SIZE).map { j => ParquetStore(i, j) })
    val dataShuf = util.Random.shuffle(data)
    println("done shuffling")

    hadoopDelete("/tmp/rdd1.parquet", sc.hadoopConfiguration, recursive = true)
    sqlContext.createDataFrame(sc.parallelize(data))
      .write.parquet("/tmp/rdd1.parquet")

    hadoopDelete("/tmp/rdd2.parquet", sc.hadoopConfiguration, recursive = true)
    println("done data1")
    sqlContext.createDataFrame(sc.parallelize(dataShuf))
      .write.parquet("/tmp/rdd2.parquet")
    println("done data2")


  }
}
