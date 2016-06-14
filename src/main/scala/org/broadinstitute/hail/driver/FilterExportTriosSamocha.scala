package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.methods._
import org.broadinstitute.hail.variant._
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.mutable.ArrayBuffer

object FilterExportTriosSamocha extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "--pop-freq", usage = "Population allele frequency annotation")
    var referenceAF: String = _

    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output root filename")
    var output: String = _

    @Args4jOption(required = true, name = "-f", aliases = Array("--fam"), usage = ".fam file")
    var famFilename: String = _

    @Args4jOption(name = "-e", aliases = Array("--export"),
      usage = ".columns file, or comma-separated list of fields/computations to be included in output")
    var additionalFields: String = _
  }

  def newOptions = new Options

  def name = "filterexporttrios samocha"

  // FIXME reference here
  def description = "Export putative de novo genotypes according to Samocha et al (REFERENCE NEEDED)"

  def requiresVDS: Boolean = true

  def supportsMultiallelic: Boolean = false

  def run(state: State, options: Options): State = {
    val sc = state.sc
    val vds = state.vds

    val ped = Pedigree.read(options.famFilename, state.hadoopConf, state.vds.sampleIds)

    val (popFrequencyT, popFrequencyF) = vds.queryVA(options.referenceAF)
    if (popFrequencyT != TDouble)
      fatal(s"population frequency should be a Double, but got `$popFrequencyT'")
    val popFrequencyFDouble: (Annotation) => Option[Double] =
      (a: Annotation) => popFrequencyF(a).map(_.asInstanceOf[Double])

    val additionalOutput = Option(options.additionalFields).map { cond =>
      val symTab = Map(
        "v" ->(0, TVariant),
        "va" ->(1, vds.vaSignature),
        "global" ->(2, vds.globalSignature),
        "proband" ->(3, TStruct(("geno", TGenotype), ("anno", vds.saSignature), ("id", TString))),
        "mother" ->(4, TStruct(("geno", TGenotype), ("anno", vds.saSignature), ("id", TString))),
        "father" ->(5, TStruct(("geno", TGenotype), ("anno", vds.saSignature), ("id", TString)))
      )

      val ec = EvalContext(symTab)
      ec.set(2, vds.globalAnnotation)

      if (cond.endsWith(".columns")) {
        val (h, fs) = ExportTSV.parseColumnsFile(ec, cond, vds.sparkContext.hadoopConfiguration)
        (ec, h, fs)
      } else {
        val (h, fs) = Parser.parseNamedArgs(cond, ec)
        (ec, h, fs)
      }
    }

    hadoopDelete(options.output, state.hadoopConf, recursive = true)
    val resultRDD = CalculateDeNovo(vds, ped.completeTrios, popFrequencyFDouble, additionalOutput.map(o => (o._1, o._3)))
    resultRDD.writeTable(options.output, header =
      Some((CalculateDeNovo.HEADER ++ additionalOutput.map(_._2)).mkString("\t")))
    state

  }
}
