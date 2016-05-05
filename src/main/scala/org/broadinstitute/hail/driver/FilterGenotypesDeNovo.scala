package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.methods._
import org.broadinstitute.hail.variant._
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.mutable.ArrayBuffer

object FilterGenotypesDeNovo extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-f", aliases = Array("--frequency"),
      usage = "Population allele frequency annotation")
    var referenceAF: String = _

    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output root filename")
    var output: String = _

    @Args4jOption(required = true, name = "-f", aliases = Array("--fam"), usage = ".fam file")
    var famFilename: String = _
  }

  def newOptions = new Options

  def name = "filtergenotypes denovo"

  def description = "Filter genotypes in current dataset"

  def run(state: State, options: Options): State = {
    val sc = state.sc
    val vds = state.vds

    val ped = Pedigree.read(options.famFilename, state.hadoopConf, state.vds.sampleIds)

    val filtered = vds.rdd.map()

    state
  }
}
