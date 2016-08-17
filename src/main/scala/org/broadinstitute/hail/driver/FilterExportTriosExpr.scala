package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.methods._
import org.broadinstitute.hail.utils.MultiArray2
import org.broadinstitute.hail.variant.Genotype
import org.kohsuke.args4j.{Option => Args4jOption}

import org.broadinstitute.hail.utils._
import scala.collection.mutable

object FilterExportTriosExpr extends Command {

  class Options extends BaseOptions {

    @Args4jOption(required = true, name = "-c", aliases = Array("--condition"), usage = "filter predicate expression")
    var condition: String = _

    @Args4jOption(required = true, name = "-e", aliases = Array("--export"), usage = "export expression")
    var export: String = _

    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output root filename")
    var output: String = _

    @Args4jOption(required = true, name = "-f", aliases = Array("--fam"), usage = ".fam file")
    var famFilename: String = _

    @Args4jOption(name = "-e", aliases = Array("--export"),
      usage = ".columns file, or comma-separated list of fields/computations to be included in output")
    var additionalFields: String = _

    @Args4jOption(required = false, name = "--remove", usage = "Remove trios matching condition")
    var remove: Boolean = false
  }

  def newOptions = new Options

  def name = "filterexporttrios expr"

  // FIXME reference here
  def description = "Export putative de novo genotypes using Hail expressions"

  def requiresVDS: Boolean = true

  def supportsMultiallelic: Boolean = true

  def run(state: State, options: Options): State = {
    val sc = state.sc
    val vds = state.vds

    val ped = Pedigree.read(options.famFilename, state.hadoopConf, state.vds.sampleIds)

    val cond = options.condition
    val exportArgs = options.export
    val keep = !options.remove

    val symTab = Map(
      "global" -> (0, vds.globalSignature),
      "v" -> (1, TVariant),
      "va" -> (2, vds.vaSignature),
      "proband" -> (3, TStruct(("geno", TGenotype), ("anno", vds.saSignature), ("id", TString))),
      "mother" -> (4, TStruct(("geno", TGenotype), ("anno", vds.saSignature), ("id", TString))),
      "father" -> (5, TStruct(("geno", TGenotype), ("anno", vds.saSignature), ("id", TString)))
    )

    val ec = EvalContext(symTab)
    ec.set(0, vds.globalAnnotation)

    val predicate = Parser.parse[Boolean](cond, ec, TBoolean)

    val (header, functions) = if (exportArgs.endsWith(".columns")) {
      val (h, fs) = Parser.parseColumnsFile(ec, exportArgs, vds.sparkContext.hadoopConfiguration)
      (Some(h), fs)
    } else {
      val (h, fs) = Parser.parseExportArgs(exportArgs, ec)
      (h, fs)
    }
    val nFn = functions.length

    hadoopDelete(options.output, state.hadoopConf, recursive = true)

    val preTrios = ped.completeTrios
    val trios = preTrios.filter(_.sex.isDefined)
    val nTrios = trios.size
    val nSamplesDiscarded = preTrios.size - nTrios

    if (nSamplesDiscarded > 0)
      warn(s"$nSamplesDiscarded ${ plural(nSamplesDiscarded, "sample") } discarded from .fam: missing from variant data set.")

    val sampleTrioRoles = mutable.Map.empty[String, List[(Int, Int)]]
    trios.zipWithIndex.map { case (t, ti) =>
      sampleTrioRoles += (t.kid -> ((ti, 0) :: sampleTrioRoles.getOrElse(t.kid, List.empty[(Int, Int)])))
      sampleTrioRoles += (t.dad -> ((ti, 1) :: sampleTrioRoles.getOrElse(t.dad, List.empty[(Int, Int)])))
      sampleTrioRoles += (t.mom -> ((ti, 2) :: sampleTrioRoles.getOrElse(t.mom, List.empty[(Int, Int)])))
    }

    val sampleTrioRolesBc = sc.broadcast(sampleTrioRoles.toMap)
    val triosBc = sc.broadcast(trios)
    val trioSexBc = sc.broadcast(trios.map(_.sex.get))

    val localIdsBc = vds.sampleIdsBc
    val annotationMap = sc.broadcast(vds.sampleIdsAndAnnotations.toMap)
    vds.rdd.mapPartitions { iter =>
      val arr = MultiArray2.fill[Genotype](trios.length, 3)(Genotype())
      val sb = new StringBuilder()
      val kidRow = MutableRow.ofSize(3)
      val momRow = MutableRow.ofSize(3)
      val dadRow = MutableRow.ofSize(3)
      ec.set(3, kidRow)
      ec.set(4, momRow)
      ec.set(5, dadRow)

      iter.flatMap { case (v, (va, gs)) =>
        val (nCalled, nAltAlleles) = (gs, localIdsBc.value).zipped.map {
          case (gt, id) =>
            sampleTrioRolesBc.value.get(id)
              .foreach { l =>
                l.foreach { case (i, j) =>
                  arr.update(i, j, gt)
                }
              }
            gt.nNonRefAlleles.map(o => (1, o)).getOrElse((0, 0))
        }.reduce[(Int, Int)] { case ((x1, x2), (y1, y2)) => (x1 + y1, x2 + y2) }

        ec.set(1, v)
        ec.set(2, va)

        (0 until nTrios)
          .flatMap { i =>
            val t = triosBc.value(i)
            val kid = t.kid
            val mom = t.mom
            val dad = t.dad

            kidRow.update(0, arr(i, 0))
            kidRow.update(1, annotationMap.value(kid))
            kidRow.update(2, kid)

            dadRow.update(0, arr(i, 1))
            dadRow.update(1, annotationMap.value(dad))
            dadRow.update(2, dad)

            momRow.update(0, arr(i, 2))
            momRow.update(1, annotationMap.value(mom))
            momRow.update(2, mom)

            if (Filter.keepThis(predicate(), keep)) {
              functions.foreachBetween({ case (t, f) => sb.append(t.str(f())) })(sb += '\t')
              Some(sb.result)
            }
            else None
          }
      }

    }.writeTable(options.output, header = header.map(_.mkString("\t")))

    state
  }
}
