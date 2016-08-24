package org.apache.spark.sql.execution.datasources.parquet

import java.io.IOException
import java.util.{List => JList}

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, JobContext}
import parquet.Preconditions
import parquet.hadoop.{ParquetInputFormat, ParquetInputSplit}
import parquet.hadoop.util.ContextUtil

import scala.collection.JavaConverters._

import java.util

/**
  * Copied and slightly modified from:
  *   org.apache.spark.sql.execution.datasources.parquet.ParquetInputFormat
  *   version 1.5.0
  *
  * Changed to sort splits by the split index in the file name
  * so that the resulting HadoopRDD has the same partitions as
  * the RDD which was written to disk.
  */
class PartitionedParquetInputFormat[T] extends ParquetInputFormat[T] {

  val partRegex = "part-r-(\\d+)-.*\\.parquet.*".r

  def getPartNumber(fname: String): Int = {
    fname match {
      case partRegex(i) => i.toInt
      case _ => throw new PathIOException("no match")
    }
  }

  override def getSplits(job: JobContext): JList[InputSplit] = {
    val splits: JList[InputSplit] = new java.util.ArrayList[InputSplit]
    val files: JList[FileStatus] = listStatus(job)

    val sorted = files.asScala.toArray.sortBy(fs => getPartNumber(fs.getPath.getName)).toList
    for (file <- sorted) {
      val path: Path = file.getPath
      val length: Long = file.getLen
      if (length != 0) {
        val blkLocations = file match {
          case lfs: LocatedFileStatus => lfs.getBlockLocations
          case _ =>
            val fs: FileSystem = path.getFileSystem(job.getConfiguration)
            fs.getFileBlockLocations(file, 0, length)
        }
        splits.add(new ParquetInputSplit(path, 0, length, length, blkLocations(0).getHosts, Array.empty[Long]))
      } else {
        splits.add(new ParquetInputSplit(path, 0, length, length, Array.empty[String], Array.empty[Long]))
      }
    }
    job.getConfiguration.setLong("mapreduce.input.num.files", files.size)
    splits
  }

//  @throws[IOException]
//  override def getSplits(jobContext: JobContext): util.List[InputSplit] = {
//    val splits: util.List[InputSplit] = new util.ArrayList[InputSplit]
//    if (ParquetInputFormat.isTaskSideMetaData(configuration)) {
//      // Although not required by the API, some clients may depend on always
//      // receiving ParquetInputSplit. Translation is required at some point.
//      import scala.collection.JavaConversions._
//      val sorted = splits.asScala.toArray.sortBy(spl => (fs.getPath.getName)).toList
//
//      for (split <- super.getSplits(jobContext)) {
//        split match {
//          case fSplit: FileSplit =>
//            splits.add(new ParquetInputSplit(fSplit.getPath, fSplit.getStart, fSplit.getStart + split.getLength,
//              fSplit.getLength, fSplit.getLocations, null))
//          case _ => throw new IllegalArgumentException("Cannot wrap non-FileSplit: " + split)
//        }
//      }
//      return splits
//    }
//    else splits.addAll(getSplits(configuration, getFooters(jobContext)))
//    splits
//  }
}
