package is.hail.variant

import java.io.FileNotFoundException

import is.hail.annotations.{Annotation, _}
import is.hail.io._
import is.hail.expr.{EvalContext, JSONAnnotationImpex, Parser, SparkAnnotationImpex, TString, TStruct, Type, _}
import is.hail.io.annotators.{BedAnnotator, IntervalListAnnotator}
import is.hail.io.plink.{FamFileConfig, PlinkLoader}
import is.hail.io.vcf.BufferedLineIterator
import is.hail.keytable.KeyTable
import is.hail.methods.{Aggregators, CalculateConcordance, DuplicateReport, Filter}
import is.hail.sparkextras.{OrderedPartitioner, OrderedRDD}
import is.hail.utils._
import is.hail.variant.Variant.orderedKey
import is.hail.variant.LocusImplicits.orderedKey
import org.apache.hadoop
import org.apache.kudu.spark.kudu.{KuduContext, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{JArray, JBool, JInt, JObject, JString, JValue, _}

import scala.collection.mutable
import scala.io.Source
import scala.language.implicitConversions
import scala.reflect.ClassTag

object VariantDataset {
  private def readMetadata(hConf: hadoop.conf.Configuration, dirname: String,
    requireParquetSuccess: Boolean = true): VariantMetadata = {
    if (!dirname.endsWith(".vds") && !dirname.endsWith(".vds/"))
      fatal(s"input path ending in `.vds' required, found `$dirname'")

    if (!hConf.exists(dirname))
      fatal(s"no VDS found at `$dirname'")

    val metadataFile = dirname + "/metadata.json.gz"
    val pqtSuccess = dirname + "/rdd.parquet/_SUCCESS"

    if (!hConf.exists(pqtSuccess) && requireParquetSuccess)
      fatal(
        s"""corrupt VDS: no parquet success indicator
           |  Unexpected shutdown occurred during `write'
           |  Recreate VDS.""".stripMargin)

    if (!hConf.exists(metadataFile))
      fatal(
        s"""corrupt or outdated VDS: invalid metadata
           |  No `metadata.json.gz' file found in VDS directory
           |  Recreate VDS with current version of Hail.""".stripMargin)

    val json = try {
      hConf.readFile(metadataFile)(
        in => JsonMethods.parse(in))
    } catch {
      case e: Throwable => fatal(
        s"""
           |corrupt VDS: invalid metadata file.
           |  Recreate VDS with current version of Hail.
           |  caught exception: ${ Main.expandException(e) }
         """.stripMargin)
    }

    val fields = json match {
      case jo: JObject => jo.obj.toMap
      case _ =>
        fatal(
          s"""corrupt VDS: invalid metadata value
             |  Recreate VDS with current version of Hail.""".stripMargin)
    }

    def getAndCastJSON[T <: JValue](fname: String)(implicit tct: ClassTag[T]): T =
      fields.get(fname) match {
        case Some(t: T) => t
        case Some(other) =>
          fatal(
            s"""corrupt VDS: invalid metadata
               |  Expected `${ tct.runtimeClass.getName }' in field `$fname', but got `${ other.getClass.getName }'
               |  Recreate VDS with current version of Hail.""".stripMargin)
        case None =>
          fatal(
            s"""corrupt VDS: invalid metadata
               |  Missing field `$fname'
               |  Recreate VDS with current version of Hail.""".stripMargin)
      }

    val version = getAndCastJSON[JInt]("version").num

    if (version != VariantSampleMatrix.fileVersion)
      fatal(
        s"""Invalid VDS: old version [$version]
           |  Recreate VDS with current version of Hail.
         """.stripMargin)

    val wasSplit = getAndCastJSON[JBool]("split").value
    val isDosage = fields.get("isDosage") match {
      case Some(t: JBool) => t.value
      case Some(other) => fatal(
        s"""corrupt VDS: invalid metadata
           |  Expected `JBool' in field `isDosage', but got `${ other.getClass.getName }'
           |  Recreate VDS with current version of Hail.""".stripMargin)
      case _ => false
    }

    val saSignature = Parser.parseType(getAndCastJSON[JString]("sample_annotation_schema").s)
    val vaSignature = Parser.parseType(getAndCastJSON[JString]("variant_annotation_schema").s)
    val globalSignature = Parser.parseType(getAndCastJSON[JString]("global_annotation_schema").s)

    val sampleInfoSchema = TStruct(("id", TString), ("annotation", saSignature))
    val sampleInfo = getAndCastJSON[JArray]("sample_annotations")
      .arr
      .map {
        case JObject(List(("id", JString(id)), ("annotation", jv: JValue))) =>
          (id, JSONAnnotationImpex.importAnnotation(jv, saSignature, "sample_annotations"))
        case other => fatal(
          s"""corrupt VDS: invalid metadata
             |  Invalid sample annotation metadata
             |  Recreate VDS with current version of Hail.""".stripMargin)
      }
      .toArray

    val globalAnnotation = JSONAnnotationImpex.importAnnotation(getAndCastJSON[JValue]("global_annotation"),
      globalSignature, "global")

    val ids = sampleInfo.map(_._1)
    val annotations = sampleInfo.map(_._2)

    VariantMetadata(ids, annotations, globalAnnotation,
      saSignature, vaSignature, globalSignature, wasSplit, isDosage)
  }


  def read(sqlContext: SQLContext, dirname: String,
    skipGenotypes: Boolean = false, skipVariants: Boolean = false): VariantDataset = {

    val sc = sqlContext.sparkContext
    val hConf = sc.hadoopConfiguration

    val metadata = readMetadata(hConf, dirname, skipGenotypes)
    val vaSignature = metadata.vaSignature

    val vaRequiresConversion = SparkAnnotationImpex.requiresConversion(vaSignature)
    val isDosage = metadata.isDosage

    val parquetFile = dirname + "/rdd.parquet"

    val orderedRDD = if (skipVariants)
      OrderedRDD.empty[Locus, Variant, (Annotation, Iterable[Genotype])](sc)
    else {
      val rdd = if (skipGenotypes)
        sqlContext.readParquetSorted(parquetFile, Some(Array("variant", "annotations")))
          .map(row => (row.getVariant(0),
            (if (vaRequiresConversion) SparkAnnotationImpex.importAnnotation(row.get(1), vaSignature) else row.get(1),
              Iterable.empty[Genotype])))
      else
        sqlContext.readParquetSorted(parquetFile)
          .map { row =>
            val v = row.getVariant(0)
            (v,
              (if (vaRequiresConversion) SparkAnnotationImpex.importAnnotation(row.get(1), vaSignature) else row.get(1),
                row.getGenotypeStream(v, 2, isDosage): Iterable[Genotype]))
          }

      val partitioner: OrderedPartitioner[Locus, Variant] =
        try {
          val jv = hConf.readFile(dirname + "/partitioner.json.gz")(JsonMethods.parse(_))
          jv.fromJSON[OrderedPartitioner[Locus, Variant]]
        } catch {
          case _: FileNotFoundException =>
            fatal("missing partitioner.json.gz when loading VDS, create with HailContext.write_partitioning.")
        }

      OrderedRDD(rdd, partitioner)
    }

    new VariantSampleMatrix[Genotype](
      if (skipGenotypes) metadata.copy(sampleIds = IndexedSeq.empty[String],
        sampleAnnotations = IndexedSeq.empty[Annotation])
      else metadata,
      orderedRDD)
  }

  def kuduRowType(vaSignature: Type): Type = TStruct("variant" -> Variant.t,
    "annotations" -> vaSignature,
    "gs" -> GenotypeStream.t,
    "sample_group" -> TString)

  def readKudu(sqlContext: SQLContext, dirname: String, tableName: String,
    master: String): VariantDataset = {

    val metadata = readMetadata(sqlContext.sparkContext.hadoopConfiguration, dirname, requireParquetSuccess = false)
    val vaSignature = metadata.vaSignature
    val isDosage = metadata.isDosage

    val df = sqlContext.read.options(
      Map("kudu.table" -> tableName, "kudu.master" -> master)).kudu

    val rowType = kuduRowType(vaSignature)
    val schema: StructType = KuduAnnotationImpex.exportType(rowType).asInstanceOf[StructType]

    // Kudu key fields are always first, so we have to reorder the fields we get back
    // to be in the column order for the flattened schema *before* we unflatten
    val indices: Array[Int] = schema.fields.zipWithIndex.map { case (field, rowIdx) =>
      df.schema.fieldIndex(field.name)
    }

    val rdd: RDD[(Variant, (Annotation, Iterable[Genotype]))] = df.rdd.map { row =>
      val importedRow = KuduAnnotationImpex.importAnnotation(
        KuduAnnotationImpex.reorder(row, indices), rowType).asInstanceOf[Row]
      val v = importedRow.getVariant(0)
      (v,
        (importedRow.get(1),
          importedRow.getGenotypeStream(v, 2, metadata.isDosage)))
    }.spanByKey().map(kv => {
      // combine variant rows with different sample groups (no shuffle)
      val variant = kv._1
      val annotations = kv._2.head._1
      // just use first annotation
      val genotypes = kv._2.flatMap(_._2) // combine genotype streams
      (variant, (annotations, genotypes))
    })
    new VariantSampleMatrix[Genotype](metadata, rdd.toOrderedRDD)
  }

  private def makeSchemaForKudu(vaSignature: Type): StructType =
    StructType(Array(
      StructField("variant", Variant.schema, nullable = false),
      StructField("annotations", vaSignature.schema, nullable = false),
      StructField("gs", GenotypeStream.schema, nullable = false),
      StructField("sample_group", StringType, nullable = false)
    ))
}

case class VariantDatasetFunctions(vds: VariantSampleMatrix[Genotype]) extends AnyVal {

  private def rdd = vds.rdd

  def makeSchema(): StructType =
    StructType(Array(
      StructField("variant", Variant.schema, nullable = false),
      StructField("annotations", vds.vaSignature.schema),
      StructField("gs", GenotypeStream.schema, nullable = false)
    ))

  def makeSchemaForKudu(): StructType =
    makeSchema().add(StructField("sample_group", StringType, nullable = false))

  def coalesce(k: Int, shuffle: Boolean = true): VariantDataset = {
    val start = if (shuffle)
      withGenotypeStream()
    else vds
    vds.copy(rdd = vds.rdd)
    start.copy(rdd = rdd.coalesce(k, shuffle = shuffle)(null).asOrderedRDD)
  }

  private def writeMetadata(sqlContext: SQLContext, dirname: String, compress: Boolean = true) {
    if (!dirname.endsWith(".vds") && !dirname.endsWith(".vds/"))
      fatal(s"output path ending in `.vds' required, found `$dirname'")

    val hConf = vds.sparkContext.hadoopConfiguration
    hConf.mkDir(dirname)

    val sb = new StringBuilder

    vds.saSignature.pretty(sb, printAttrs = true, compact = true)
    val saSchemaString = sb.result()

    sb.clear()
    vds.vaSignature.pretty(sb, printAttrs = true, compact = true)
    val vaSchemaString = sb.result()

    sb.clear()
    vds.globalSignature.pretty(sb, printAttrs = true, compact = true)
    val globalSchemaString = sb.result()

    val sampleInfoSchema = TStruct(("id", TString), ("annotation", vds.saSignature))
    val sampleInfoJson = JArray(
      vds.sampleIdsAndAnnotations
        .map { case (id, annotation) =>
          JObject(List(("id", JString(id)), ("annotation", JSONAnnotationImpex.exportAnnotation(annotation, vds.saSignature))))
        }
        .toList
    )

    val json = JObject(
      ("version", JInt(VariantSampleMatrix.fileVersion)),
      ("split", JBool(vds.wasSplit)),
      ("isDosage", JBool(vds.isDosage)),
      ("sample_annotation_schema", JString(saSchemaString)),
      ("variant_annotation_schema", JString(vaSchemaString)),
      ("global_annotation_schema", JString(globalSchemaString)),
      ("sample_annotations", sampleInfoJson),
      ("global_annotation", JSONAnnotationImpex.exportAnnotation(vds.globalAnnotation, vds.globalSignature))
    )

    hConf.writeTextFile(dirname + "/metadata.json.gz")(Serialization.writePretty(json, _))
  }

  def write(sqlContext: SQLContext, dirname: String, compress: Boolean = true) {
    writeMetadata(sqlContext, dirname, compress)

    val vaSignature = vds.vaSignature
    val vaRequiresConversion = SparkAnnotationImpex.requiresConversion(vaSignature)

    val ordered = vds.rdd.asOrderedRDD

    sqlContext.sparkContext.hadoopConfiguration.writeTextFile(dirname + "/partitioner.json.gz") { out =>
      Serialization.write(ordered.orderedPartitioner.toJSON, out)
    }

    val isDosage = vds.isDosage
    val rowRDD = ordered.map { case (v, (va, gs)) =>
      Row.fromSeq(Array(v.toRow,
        if (vaRequiresConversion) SparkAnnotationImpex.exportAnnotation(va, vaSignature) else va,
        gs.toGenotypeStream(v, isDosage, compress).toRow))
    }
    sqlContext.createDataFrame(rowRDD, makeSchema())
      .write.parquet(dirname + "/rdd.parquet")
    // .saveAsParquetFile(dirname + "/rdd.parquet")
  }

  def writeKudu(sqlContext: SQLContext, dirname: String, tableName: String,
    master: String, vcfSeqDict: String, rowsPerPartition: Int,
    sampleGroup: String, compress: Boolean = true, drop: Boolean = false) {

    writeMetadata(sqlContext, dirname, compress)

    val vaSignature = vds.vaSignature
    val isDosage = vds.isDosage

    val rowType = VariantDataset.kuduRowType(vaSignature)
    val rowRDD = vds.rdd
      .map { case (v, (va, gs)) =>
        KuduAnnotationImpex.exportAnnotation(Annotation(
          v.toRow,
          va,
          gs.toGenotypeStream(v, isDosage, compress).toRow,
          sampleGroup), rowType).asInstanceOf[Row]
      }

    val schema: StructType = KuduAnnotationImpex.exportType(rowType).asInstanceOf[StructType]
    println(s"schema = $schema")
    val df = sqlContext.createDataFrame(rowRDD, schema)

    val kuduContext = new KuduContext(master)
    if (drop) {
      KuduUtils.dropTable(master, tableName)
      Thread.sleep(10 * 1000) // wait to avoid overwhelming Kudu service queue
    }
    if (!KuduUtils.tableExists(master, tableName)) {
      val hConf = sqlContext.sparkContext.hadoopConfiguration
      val headerLines = hConf.readFile(vcfSeqDict) { s =>
        Source.fromInputStream(s)
          .getLines()
          .takeWhile { line => line(0) == '#' }
          .toArray
      }
      val codec = new htsjdk.variant.vcf.VCFCodec()
      val seqDict = codec.readHeader(new BufferedLineIterator(headerLines.iterator.buffered))
        .getHeaderValue
        .asInstanceOf[htsjdk.variant.vcf.VCFHeader]
        .getSequenceDictionary

      val keys = Seq("variant__contig", "variant__start", "variant__ref",
        "variant__altAlleles_0__alt", "sample_group")
      kuduContext.createTable(tableName, schema, keys,
        KuduUtils.createTableOptions(schema, keys, seqDict, rowsPerPartition))
    }
    df.write
      .options(Map("kudu.master" -> master, "kudu.table" -> tableName))
      .mode("append")
      // FIXME inlined since .kudu wouldn't work for some reason
      .format("org.apache.kudu.spark.kudu").save

    println("Written to Kudu")
  }

  def eraseSplit: VariantDataset = {
    if (vds.wasSplit) {
      val (newSignatures1, f1) = vds.deleteVA("wasSplit")
      val vds1 = vds.copy(vaSignature = newSignatures1)
      val (newSignatures2, f2) = vds1.deleteVA("aIndex")
      vds1.copy(wasSplit = false,
        vaSignature = newSignatures2,
        rdd = vds1.rdd.mapValuesWithKey { case (v, (va, gs)) =>
          (f2(f1(va)), gs.lazyMap(g => g.copy(fakeRef = false)))
        }.asOrderedRDD)
    } else
      vds
  }

  def withGenotypeStream(compress: Boolean = true): VariantDataset = {
    val isDosage = vds.isDosage
    vds.copy(rdd = vds.rdd.mapValuesWithKey[(Annotation, Iterable[Genotype])] { case (v, (va, gs)) =>
      (va, gs.toGenotypeStream(v, isDosage, compress = compress))
    }.asOrderedRDD)
  }

  def filterVariantsExpr(cond: String, keep: Boolean): VariantDataset = {
    val localGlobalAnnotation = vds.globalAnnotation
    val ec = Aggregators.variantEC(vds)

    val f: () => Option[Boolean] = Parser.parseTypedExpr[Boolean](cond, ec)

    val aggregatorOption = Aggregators.buildVariantAggregations(vds, ec)

    val p = (v: Variant, va: Annotation, gs: Iterable[Genotype]) => {
      aggregatorOption.foreach(f => f(v, va, gs))

      ec.setAll(localGlobalAnnotation, v, va)
      Filter.keepThis(f(), keep)
    }

    vds.filterVariants(p)
  }

  def aggregateIntervals(intervalList: String, expr: String, out: String) {

    val vas = vds.vaSignature
    val sas = vds.saSignature
    val localGlobalAnnotation = vds.globalAnnotation

    val aggregationST = Map(
      "global" -> (0, vds.globalSignature),
      "interval" -> (1, TInterval),
      "v" -> (2, TVariant),
      "va" -> (3, vds.vaSignature))
    val symTab = Map(
      "global" -> (0, vds.globalSignature),
      "interval" -> (1, TInterval),
      "variants" -> (2, TAggregable(TVariant, aggregationST)))

    val ec = EvalContext(symTab)
    ec.set(1, vds.globalAnnotation)

    val (names, _, f) = Parser.parseExportExprs(expr, ec)

    if (names.isEmpty)
      fatal("this module requires one or more named expr arguments")

    val (zVals, seqOp, combOp, resultOp) = Aggregators.makeFunctions[(Interval[Locus], Variant, Annotation)](ec, { case (ec, (i, v, va)) =>
      ec.setAll(localGlobalAnnotation, i, v, va)
    })

    val iList = IntervalListAnnotator.read(intervalList, vds.sparkContext.hadoopConfiguration)
    val iListBc = vds.sparkContext.broadcast(iList)

    val results = vds.variantsAndAnnotations.flatMap { case (v, va) =>
      iListBc.value.query(v.locus).map { i => (i, (i, v, va)) }
    }
      .aggregateByKey(zVals)(seqOp, combOp)
      .collectAsMap()

    vds.sparkContext.hadoopConfiguration.writeTextFile(out) { out =>
      val sb = new StringBuilder
      sb.append("Contig")
      sb += '\t'
      sb.append("Start")
      sb += '\t'
      sb.append("End")
      names.foreach { col =>
        sb += '\t'
        sb.append(col)
      }
      sb += '\n'

      iList.toIterator
        .foreachBetween { interval =>

          sb.append(interval.start.contig)
          sb += '\t'
          sb.append(interval.start.position)
          sb += '\t'
          sb.append(interval.end.position)
          val res = results.getOrElse(interval, zVals)
          resultOp(res)

          ec.setAll(localGlobalAnnotation, interval)
          f().foreach { field =>
            sb += '\t'
            sb.append(field)
          }
        }(sb += '\n')

      out.write(sb.result())
    }

  }

  def annotateAllelesExpr(expr: String, propagateGQ: Boolean = false): VariantDataset = {
    val isDosage = vds.isDosage

    val (vas2, insertIndex) = vds.vaSignature.insert(TInt, "aIndex")
    val (vas3, insertSplit) = vas2.insert(TBoolean, "wasSplit")
    val localGlobalAnnotation = vds.globalAnnotation

    val aggregationST = Map(
      "global" -> (0, vds.globalSignature),
      "v" -> (1, TVariant),
      "va" -> (2, vas3),
      "g" -> (3, TGenotype),
      "s" -> (4, TSample),
      "sa" -> (5, vds.saSignature))
    val ec = EvalContext(Map(
      "global" -> (0, vds.globalSignature),
      "v" -> (1, TVariant),
      "va" -> (2, vas3),
      "gs" -> (3, TAggregable(TGenotype, aggregationST))))

    val (paths, types, f) = Parser.parseAnnotationExprs(expr, ec, Some(Annotation.VARIANT_HEAD))

    val inserterBuilder = mutable.ArrayBuilder.make[Inserter]
    val finalType = (paths, types).zipped.foldLeft(vds.vaSignature) { case (vas, (ids, signature)) =>
      val (s, i) = vas.insert(TArray(signature), ids)
      inserterBuilder += i
      s
    }
    val inserters = inserterBuilder.result()

    val aggregateOption = Aggregators.buildVariantAggregations(vds, ec)

    vds.mapAnnotations { case (v, va, gs) =>

      val annotations = SplitMulti.split(v, va, gs,
        propagateGQ = propagateGQ,
        compress = true,
        keepStar = true,
        isDosage = isDosage,
        insertSplitAnnots = { (va, index, wasSplit) =>
          insertSplit(insertIndex(va, Some(index)), Some(wasSplit))
        })
        .map({
          case (v, (va, gs)) =>
            ec.setAll(localGlobalAnnotation, v, va)
            aggregateOption.foreach(f => f(v, va, gs))
            f()
        }).toArray

      inserters.zipWithIndex.foldLeft(va) {
        case (va, (inserter, i)) =>
          inserter(va, Some(annotations.map(_ (i).getOrElse(Annotation.empty)).toArray[Any]: IndexedSeq[Any]))
      }

    }.copy(vaSignature = finalType)
  }

  def annotateGlobalExpr(expr: String): VariantDataset = {
    val ec = EvalContext(Map(
      "global" -> (0, vds.globalSignature)))

    val (paths, types, f) = Parser.parseAnnotationExprs(expr, ec, Option(Annotation.GLOBAL_HEAD))

    val inserterBuilder = mutable.ArrayBuilder.make[Inserter]

    val finalType = (paths, types).zipped.foldLeft(vds.globalSignature) { case (v, (ids, signature)) =>
      val (s, i) = v.insert(signature, ids)
      inserterBuilder += i
      s
    }

    val inserters = inserterBuilder.result()

    ec.set(0, vds.globalAnnotation)
    val ga = inserters
      .zip(f())
      .foldLeft(vds.globalAnnotation) { case (a, (ins, res)) =>
        ins(a, res)
      }

    vds.copy(globalAnnotation = ga,
      globalSignature = finalType)
  }

  def annotateGlobalList(path: String, root: String, asSet: Boolean = false): VariantDataset = {
    val textList = vds.sparkContext.hadoopConfiguration.readFile(path) { in =>
      Source.fromInputStream(in)
        .getLines()
        .toArray
    }

    val (sig, toInsert) =
      if (asSet)
        (TSet(TString), textList.toSet)
      else
        (TArray(TString), textList: IndexedSeq[String])

    val rootPath = Parser.parseAnnotationRoot(root, "global")

    val (newGlobalSig, inserter) = vds.insertGlobal(sig, rootPath)

    vds.copy(
      globalAnnotation = inserter(vds.globalAnnotation, Some(toInsert)),
      globalSignature = newGlobalSig)
  }

  def annotateGlobalTable(path: String, root: String,
    config: TextTableConfiguration = TextTableConfiguration()): VariantDataset = {
    val annotationPath = Parser.parseAnnotationRoot(root, Annotation.GLOBAL_HEAD)

    val (struct, rdd) = TextTableReader.read(vds.sparkContext)(Array(path), config)
    val arrayType = TArray(struct)

    val (finalType, inserter) = vds.insertGlobal(arrayType, annotationPath)

    val table = rdd
      .map(_.value)
      .collect(): IndexedSeq[Annotation]

    vds.copy(
      globalAnnotation = inserter(vds.globalAnnotation, Some(table)),
      globalSignature = finalType)
  }

  def annotateSamplesExpr(expr: String): VariantDataset = {
    val ec = Aggregators.sampleEC(vds)

    val (paths, types, f) = Parser.parseAnnotationExprs(expr, ec, Some(Annotation.SAMPLE_HEAD))

    val inserterBuilder = mutable.ArrayBuilder.make[Inserter]
    val finalType = (paths, types).zipped.foldLeft(vds.saSignature) { case (sas, (ids, signature)) =>
      val (s, i) = sas.insert(signature, ids)
      inserterBuilder += i
      s
    }
    val inserters = inserterBuilder.result()

    val sampleAggregationOption = Aggregators.buildSampleAggregations(vds, ec)

    ec.set(0, vds.globalAnnotation)
    val newAnnotations = vds.sampleIdsAndAnnotations.map { case (s, sa) =>
      sampleAggregationOption.foreach(f => f.apply(s))
      ec.set(1, s)
      ec.set(2, sa)
      f().zip(inserters)
        .foldLeft(sa) { case (sa, (v, inserter)) =>
          inserter(sa, v)
        }
    }

    vds.copy(
      sampleAnnotations = newAnnotations,
      saSignature = finalType
    )
  }

  def annotateSamplesFam(path: String, root: String, config: FamFileConfig = FamFileConfig()): VariantDataset = {
    if (!path.endsWith(".fam"))
      fatal("input file must end in .fam")

    val (info, signature) = PlinkLoader.parseFam(path, config, vds.sparkContext.hadoopConfiguration)

    val duplicateIds = info.map(_._1).duplicates().toArray
    if (duplicateIds.nonEmpty) {
      val n = duplicateIds.length
      fatal(
        s"""found $n duplicate sample ${ plural(n, "id") }:
           |  @1""".stripMargin, duplicateIds)
    }

    vds.annotateSamples(info.toMap, signature, root)
  }

  def annotateSamplesList(path: String, root: String): VariantDataset = {

    val samplesInList = vds.sparkContext.hadoopConfiguration.readLines(path) { lines =>
      if (lines.isEmpty)
        warn(s"Empty annotation file given: $path")

      lines.map(_.value).toSet
    }

    val sampleAnnotations = vds.sampleIds.map { s => (s, samplesInList.contains(s)) }.toMap
    vds.annotateSamples(sampleAnnotations, TBoolean, root)
  }

  def annotateSamplesTable(path: String, sampleExpr: String,
    root: Option[String] = None, code: Option[String] = None,
    config: TextTableConfiguration = TextTableConfiguration()): VariantDataset = {

    val (isCode, annotationExpr) = (root, code) match {
      case (Some(r), None) => (false, r)
      case (None, Some(c)) => (true, c)
      case _ => fatal("this module requires one of `root' or 'code', but not both")
    }

    val (struct, rdd) = TextTableReader.read(vds.sparkContext)(Array(path), config)

    val (finalType, inserter): (Type, (Annotation, Option[Annotation]) => Annotation) =
      if (isCode) {
        val ec = EvalContext(Map(
          "sa" -> (0, vds.saSignature),
          "table" -> (1, struct)))
        buildInserter(annotationExpr, vds.saSignature, ec, Annotation.SAMPLE_HEAD)
      } else
        vds.insertSA(struct, Parser.parseAnnotationRoot(annotationExpr, Annotation.SAMPLE_HEAD))

    val sampleQuery = struct.parseInStructScope[String](sampleExpr)

    val map = rdd
      .flatMap {
        _.map { a =>
          sampleQuery(a).map(s => (s, a))
        }.value
      }
      .collect()
      .toMap

    vds.annotateSamples(map.get _, finalType, inserter)
  }

  def annotateSamplesVDS(other: VariantDataset,
    root: Option[String] = None,
    code: Option[String] = None): VariantDataset = {

    val (isCode, annotationExpr) = (root, code) match {
      case (Some(r), None) => (false, r)
      case (None, Some(c)) => (true, c)
      case _ => fatal("this module requires one of `root' or 'code', but not both")
    }

    val (finalType, inserter): (Type, (Annotation, Option[Annotation]) => Annotation) =
      if (isCode) {
        val ec = EvalContext(Map(
          "sa" -> (0, vds.saSignature),
          "vds" -> (1, other.saSignature)))
        Annotation.buildInserter(annotationExpr, vds.saSignature, ec, Annotation.SAMPLE_HEAD)
      } else
        vds.insertSA(other.saSignature, Parser.parseAnnotationRoot(annotationExpr, Annotation.SAMPLE_HEAD))

    val m = other.sampleIdsAndAnnotations.toMap
    vds
      .annotateSamples(m.get _, finalType, inserter)
  }

  def annotateVariantsBED(path: String, root: String, all: Boolean = false): VariantDataset = {
    val annotationPath = Parser.parseAnnotationRoot(root, Annotation.VARIANT_HEAD)
    BedAnnotator(path, vds.sparkContext.hadoopConfiguration) match {
      case (is, None) =>
        vds.annotateIntervals(is, annotationPath)

      case (is, Some((t, m))) =>
        vds.annotateIntervals(is, t, m, all = all, annotationPath)
    }
  }

  def annotateVariantsExpr(expr: String): VariantDataset = {
    val localGlobalAnnotation = vds.globalAnnotation

    val ec = Aggregators.variantEC(vds)
    val (paths, types, f) = Parser.parseAnnotationExprs(expr, ec, Some(Annotation.VARIANT_HEAD))

    val inserterBuilder = mutable.ArrayBuilder.make[Inserter]
    val finalType = (paths, types).zipped.foldLeft(vds.vaSignature) { case (vas, (ids, signature)) =>
      val (s, i) = vas.insert(signature, ids)
      inserterBuilder += i
      s
    }
    val inserters = inserterBuilder.result()

    val aggregateOption = Aggregators.buildVariantAggregations(vds, ec)

    vds.mapAnnotations { case (v, va, gs) =>
      ec.setAll(localGlobalAnnotation, v, va)

      aggregateOption.foreach(f => f(v, va, gs))
      f().zip(inserters)
        .foldLeft(va) { case (va, (v, inserter)) =>
          inserter(va, v)
        }
    }.copy(vaSignature = finalType)
  }

  def annotateVariantsIntervals(path: String, root: String, all: Boolean = false): VariantDataset = {
    val annotationPath = Parser.parseAnnotationRoot(root, Annotation.VARIANT_HEAD)

    IntervalListAnnotator(path, vds.sparkContext.hadoopConfiguration) match {
      case (is, Some((m, t))) =>
        vds.annotateIntervals(is, m, t, all = all, annotationPath)

      case (is, None) =>
        vds.annotateIntervals(is, annotationPath)
    }
  }

  def annotateVariantsLoci(path: String, locusExpr: String,
    root: Option[String] = None, code: Option[String] = None,
    config: TextTableConfiguration = TextTableConfiguration()): VariantDataset =
    annotateVariantsLoci(List(path), locusExpr, root, code, config)


  def annotateVariantsLoci(paths: Seq[String], locusExpr: String,
    root: Option[String] = None, code: Option[String] = None,
    config: TextTableConfiguration = TextTableConfiguration()): VariantDataset = {
    val files = vds.sparkContext.hadoopConfiguration.globAll(paths)
    if (files.isEmpty)
      fatal("Arguments referred to no files")

    val (struct, rdd) = TextTableReader.read(vds.sparkContext)(files, config, vds.nPartitions)

    val (isCode, annotationExpr) = (root, code) match {
      case (Some(r), None) => (false, r)
      case (None, Some(c)) => (true, c)
      case _ => fatal("this module requires one of `root' or 'code', but not both")
    }

    val (finalType, inserter): (Type, (Annotation, Option[Annotation]) => Annotation) =
      if (isCode) {
        val ec = EvalContext(Map(
          "va" -> (0, vds.vaSignature),
          "table" -> (1, struct)))
        Annotation.buildInserter(annotationExpr, vds.vaSignature, ec, Annotation.VARIANT_HEAD)
      } else vds.insertVA(struct, Parser.parseAnnotationRoot(annotationExpr, Annotation.VARIANT_HEAD))

    val locusQuery = struct.parseInStructScope[Locus](locusExpr)


    import is.hail.variant.LocusImplicits.orderedKey
    val lociRDD = rdd.flatMap {
      _.map { a =>
        locusQuery(a).map(l => (l, a))
      }.value
    }.toOrderedRDD(vds.rdd.orderedPartitioner.mapMonotonic)

    vds.annotateLoci(lociRDD, finalType, inserter)
  }

  def annotateVariantsTable(path: String, variantExpr: String,
    root: Option[String] = None, code: Option[String] = None,
    config: TextTableConfiguration = TextTableConfiguration()): VariantDataset =
    annotateVariantsTable(List(path), variantExpr, root, code, config)

  def annotateVariantsTable(paths: Seq[String], variantExpr: String,
    root: Option[String] = None, code: Option[String] = None,
    config: TextTableConfiguration = TextTableConfiguration()): VariantDataset = {
    val files = vds.sparkContext.hadoopConfiguration.globAll(paths)
    if (files.isEmpty)
      fatal("Arguments referred to no files")

    val (struct, rdd) = TextTableReader.read(vds.sparkContext)(files, config, vds.nPartitions)

    val (isCode, annotationExpr) = (root, code) match {
      case (Some(r), None) => (false, r)
      case (None, Some(c)) => (true, c)
      case _ => fatal("this module requires one of `root' or 'code', but not both")
    }

    val (finalType, inserter): (Type, (Annotation, Option[Annotation]) => Annotation) =
      if (isCode) {
        val ec = EvalContext(Map(
          "va" -> (0, vds.vaSignature),
          "table" -> (1, struct)))
        Annotation.buildInserter(annotationExpr, vds.vaSignature, ec, Annotation.VARIANT_HEAD)
      } else vds.insertVA(struct, Parser.parseAnnotationRoot(annotationExpr, Annotation.VARIANT_HEAD))

    val variantQuery = struct.parseInStructScope[Variant](variantExpr)

    val keyedRDD = rdd.flatMap {
      _.map { a =>
        variantQuery(a).map(v => (v, a))
      }.value
    }.toOrderedRDD(vds.rdd.orderedPartitioner)

    vds.annotateVariants(keyedRDD, finalType, inserter)
  }

  def annotateVariantsVDS(other: VariantDataset,
    root: Option[String] = None, code: Option[String] = None): VariantDataset = {

    val (isCode, annotationExpr) = (root, code) match {
      case (Some(r), None) => (false, r)
      case (None, Some(c)) => (true, c)
      case _ => fatal("this module requires one of `root' or 'code', but not both")
    }

    val (finalType, inserter): (Type, (Annotation, Option[Annotation]) => Annotation) =
      if (isCode) {
        val ec = EvalContext(Map(
          "va" -> (0, vds.vaSignature),
          "vds" -> (1, other.vaSignature)))
        Annotation.buildInserter(annotationExpr, vds.vaSignature, ec, Annotation.VARIANT_HEAD)
      } else vds.insertVA(other.vaSignature, Parser.parseAnnotationRoot(annotationExpr, Annotation.VARIANT_HEAD))

    vds.annotateVariants(other.variantsAndAnnotations, finalType, inserter)
  }

  def concordance(other: VariantDataset): (IndexedSeq[IndexedSeq[Long]], VariantDataset, VariantDataset) = {
    require(vds.wasSplit && other.wasSplit, "method `concordance' requires both left and right datasets to be split.")

    CalculateConcordance(vds, other)
  }

  def count(countGenotypes: Boolean = false): CountResult = {
    val (nVariants, nCalled) =
      if (countGenotypes) {
        val (nVar, nCalled) = vds.rdd.map { case (v, (va, gs)) =>
          (1L, gs.count(_.isCalled).toLong)
        }.fold((0L, 0L)) { (comb, x) =>
          (comb._1 + x._1, comb._2 + x._2)
        }
        (nVar, Some(nCalled))
      } else
        (vds.countVariants, None)

    CountResult(vds.nSamples, nVariants, nCalled)
  }

  def deduplicate(): VariantDataset = {
    DuplicateReport.initialize()

    val acc = DuplicateReport.accumulator
    vds.copy(rdd = vds.rdd.mapPartitions({ it =>
      new SortedDistinctPairIterator(it, (v: Variant) => acc += v)
    }, preservesPartitioning = true).asOrderedRDD)
  }

  def downsampleVariants(keep: Long): VariantDataset = {
    vds.sampleVariants(keep.toDouble / vds.countVariants())
  }

  def exportGen(path: String) {
    require(vds.wasSplit, "method `exportGen' requires a split dataset")

    def writeSampleFile() {
      //FIXME: should output all relevant sample annotations such as phenotype, gender, ...
      vds.sparkContext.hadoopConfiguration.writeTable(path + ".sample",
        "ID_1 ID_2 missing" :: "0 0 0" :: vds.sampleIds.map(s => s"$s $s 0").toList)
    }


    def formatDosage(d: Double): String = d.formatted("%.4f")

    val emptyDosage = Array(0d, 0d, 0d)

    def appendRow(sb: StringBuilder, v: Variant, va: Annotation, gs: Iterable[Genotype], rsidQuery: Querier, varidQuery: Querier) {
      sb.append(v.contig)
      sb += ' '
      sb.append(varidQuery(va).getOrElse(v.toString))
      sb += ' '
      sb.append(rsidQuery(va).getOrElse("."))
      sb += ' '
      sb.append(v.start)
      sb += ' '
      sb.append(v.ref)
      sb += ' '
      sb.append(v.alt)

      for (gt <- gs) {
        val dosages = gt.dosage.getOrElse(emptyDosage)
        sb += ' '
        sb.append(formatDosage(dosages(0)))
        sb += ' '
        sb.append(formatDosage(dosages(1)))
        sb += ' '
        sb.append(formatDosage(dosages(2)))
      }
    }

    def writeGenFile() {
      val varidSignature = vds.vaSignature.getOption("varid")
      val varidQuery: Querier = varidSignature match {
        case Some(_) => val (t, q) = vds.queryVA("va.varid")
          t match {
            case TString => q
            case _ => a => None
          }
        case None => a => None
      }

      val rsidSignature = vds.vaSignature.getOption("rsid")
      val rsidQuery: Querier = rsidSignature match {
        case Some(_) => val (t, q) = vds.queryVA("va.rsid")
          t match {
            case TString => q
            case _ => a => None
          }
        case None => a => None
      }

      val isDosage = vds.isDosage

      vds.rdd.mapPartitions { it: Iterator[(Variant, (Annotation, Iterable[Genotype]))] =>
        val sb = new StringBuilder
        it.map { case (v, (va, gs)) =>
          sb.clear()
          appendRow(sb, v, va, gs, rsidQuery, varidQuery)
          sb.result()
        }
      }.writeTable(path + ".gen", None)
    }

    writeSampleFile()
    writeGenFile()
  }

  def exportGenotypes(path: String, expr: String, typeFile: Boolean,
    printRef: Boolean = false, printMissing: Boolean = false) {
    val symTab = Map(
      "v" -> (0, TVariant),
      "va" -> (1, vds.vaSignature),
      "s" -> (2, TSample),
      "sa" -> (3, vds.saSignature),
      "g" -> (4, TGenotype),
      "global" -> (5, vds.globalSignature))

    val ec = EvalContext(symTab)
    ec.set(5, vds.globalAnnotation)
    val (names, ts, f) = Parser.parseExportExprs(expr, ec)

    val hadoopConf = vds.sparkContext.hadoopConfiguration
    if (typeFile) {
      hadoopConf.delete(path + ".types", recursive = false)
      val typeInfo = names
        .getOrElse(ts.indices.map(i => s"_$i").toArray)
        .zip(ts)
      exportTypes(path + ".types", hadoopConf, typeInfo)
    }

    hadoopConf.delete(path, recursive = true)

    val sampleIdsBc = vds.sparkContext.broadcast(vds.sampleIds)
    val sampleAnnotationsBc = vds.sparkContext.broadcast(vds.sampleAnnotations)

    val localPrintRef = printRef
    val localPrintMissing = printMissing

    val filterF: Genotype => Boolean =
      g => (!g.isHomRef || localPrintRef) && (!g.isNotCalled || localPrintMissing)

    val lines = vds.mapPartitionsWithAll { it =>
      val sb = new StringBuilder()
      it
        .filter { case (v, va, s, sa, g) => filterF(g) }
        .map { case (v, va, s, sa, g) =>
          ec.setAll(v, va, s, sa, g)
          sb.clear()

          f().foreachBetween(x => sb.append(x))(sb += '\t')
          sb.result()
        }
    }.writeTable(path, names.map(_.mkString("\t")))

  }
}