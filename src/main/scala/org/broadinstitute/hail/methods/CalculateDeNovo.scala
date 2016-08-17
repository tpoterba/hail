package org.broadinstitute.hail.methods

import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr.EvalContext
import org.broadinstitute.hail.utils.MultiArray2

import scala.collection.mutable

object CalculateDeNovo {

  val PRIOR = 1.0 / 30000000

  val MIN_PRIOR = 100.0 / 30000000

  val dpCutoff = .10

  def validGt(g: Genotype): Boolean = g.pl.isDefined && g.ad.isDefined && g.gq.isDefined && g.dp.isDefined

  def validParent(g: Genotype): Boolean = g.isHomRef && validGt(g)

  def validProband(g: Genotype): Boolean = g.isHet && validGt(g)

  val HEADER = Array("Chr", "Pos", "Ref", "Alt", "Proband_ID", "Father_ID",
    "Mother_ID", "Proband_Sex", "Proband_AffectedStatus", "Validation_likelihood", "Proband_PL_AA",
    "Father_PL_AB", "Mother_PL_AB", "Proband_AD_Ratio", "Father_AD_Ratio",
    "Mother_AD_Ratio", "DP_Proband", "DP_Father", "DP_Mother", "DP_Ratio",
    "P_de_novo")

  def apply(vds: VariantDataset, preTrios: IndexedSeq[CompleteTrio],
    popFreqFn: (Annotation) => Option[Double],
    additionalQueries: Option[(EvalContext, Array[(Type, () => Option[Any])])]): RDD[String] = {
    val trios = preTrios.filter(_.sex.isDefined)
    val nSamplesDiscarded = preTrios.size - trios.size
    val nTrios = trios.size

    if (nSamplesDiscarded > 0)
      warn(s"$nSamplesDiscarded ${ plural(nSamplesDiscarded, "sample") } discarded from .fam: missing from variant data set.")
    val sampleTrioRoles = mutable.Map.empty[String, List[(Int, Int)]]


    // need a map from Sample position(int) to (int, int)
    trios.zipWithIndex.map { case (t, ti) =>
      sampleTrioRoles += (t.kid -> ((ti, 0) :: sampleTrioRoles.getOrElse(t.kid, List.empty[(Int, Int)])))
      sampleTrioRoles += (t.dad -> ((ti, 1) :: sampleTrioRoles.getOrElse(t.dad, List.empty[(Int, Int)])))
      sampleTrioRoles += (t.mom -> ((ti, 2) :: sampleTrioRoles.getOrElse(t.mom, List.empty[(Int, Int)])))
    }

    val sc = vds.sparkContext

    val sampleTrioRolesBc = sc.broadcast(sampleTrioRoles.toMap)
    val triosBc = sc.broadcast(trios)
    val trioSexBc = sc.broadcast(trios.map(_.sex.get))

    val localGlobal = vds.globalAnnotation
    val localIdsBc = vds.sampleIdsBc
    val annotationMap = sc.broadcast(vds.sampleIdsAndAnnotations.toMap)
    vds.rdd.mapPartitions { iter =>
      val arr = MultiArray2.fill[Genotype](trios.length, 3)(Genotype())
      val sb = new StringBuilder()

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

        val datasetFrequency = nAltAlleles.toDouble / (nCalled * 2)

        val popFrequency = popFreqFn(va).getOrElse(0d)
        if (popFrequency < 0 || popFrequency > 1)
          fatal(
            s"""invalid population frequency value `$popFrequency' for variant $v
                |  Population prior must fall between 0 and 1.""".stripMargin)

        val frequency = math.max(math.max(datasetFrequency, popFrequency), MIN_PRIOR)

        (0 until nTrios)
          .filter(i => validProband(arr(i, 0)) && validParent(arr(i, 1)) && validParent(arr(i, 2)))
          .flatMap { i =>
            val kidGt = arr(i, 0).toCompleteGenotype.get
            val dadGt = arr(i, 1).toCompleteGenotype.get
            val momGt = arr(i, 2).toCompleteGenotype.get

            // fixme precomputed
            val kidP = kidGt.pl.map(x => math.pow(10, -x / 10d))
            val dadP = dadGt.pl.map(x => math.pow(10, -x / 10d))
            val momP = momGt.pl.map(x => math.pow(10, -x / 10d))

            val pDeNovoData = dadP(0) * momP(0) * kidP(1) * PRIOR

            val pDataOneHet = (dadP(1) * momP(0) + dadP(0) * momP(1)) * kidP(1)
            val pOneParentHet = 1 - math.pow(1 - frequency, 4)
            val pMhipData = pDataOneHet * pOneParentHet

            val pTrueDeNovo = pDeNovoData / (pDeNovoData + pMhipData)

            val momAdRatio = momGt.ad(1).toDouble / momGt.ad.sum

            val dadAdRatio = dadGt.ad(1).toDouble / dadGt.ad.sum

            val kidAdRatio = kidGt.ad(1).toDouble / kidGt.ad.sum

            val kidDp = kidGt.dp
            val dpRatio = kidDp.toDouble / (momGt.dp + dadGt.dp)

            // Below is the core calling algorithm
            val genotypeAnnotation = if (v.altAllele.isIndel) {
              if ((pTrueDeNovo > 0.99) && (kidAdRatio > 0.3) && (nAltAlleles == 1))
                Some("HIGH_indel")
              else if ((pTrueDeNovo > 0.5) && (kidAdRatio > 0.3) && (nAltAlleles <= 5))
                Some("MEDIUM_indel")
              else if ((pTrueDeNovo > 0.05) && (kidAdRatio > 0.2))
                Some("LOW_indel")
              else None
            } else {
              if ((pTrueDeNovo > 0.99) && (kidAdRatio > 0.3) && (dpRatio > 0.2) ||
                ((pTrueDeNovo > 0.99) && (kidAdRatio > 0.3) && (nAltAlleles == 1)) ||
                ((pTrueDeNovo > 0.5) && (kidAdRatio >= 0.3) && (nAltAlleles < 10) && (kidDp >= 10))
              )
                Some("HIGH_SNV")
              else if ((pTrueDeNovo > 0.5) && (kidAdRatio > 0.3) ||
                ((pTrueDeNovo > 0.5) && (kidAdRatio > 0.2) && (nAltAlleles == 1))
              )
                Some("MEDIUM_SNV")
              else if ((pTrueDeNovo > 0.05) && (kidAdRatio > 0.2))
                Some("LOW_SNV")
              else None
            }

            genotypeAnnotation.map { identifier =>
              sb.clear()
              val kidId = triosBc.value(i).kid
              val dadId = triosBc.value(i).dad
              val momId = triosBc.value(i).mom

              sb.append(v.contig)
              sb += '\t'
              sb.append(v.start)
              sb += '\t'
              sb.append(v.ref)
              sb += '\t'
              sb.append(v.alt)
              sb += '\t'
              sb.append(kidId)
              sb += '\t'
              sb.append(dadId)
              sb += '\t'
              sb.append(momId)
              sb += '\t'
              sb.append(triosBc.value(i).sex.getOrElse("NA"))
              sb += '\t'
              sb.append(triosBc.value(i).pheno.getOrElse("NA"))
              sb += '\t'
              sb.append(identifier)
              sb += '\t'
              sb.append(kidGt.pl(0))
              sb += '\t'
              sb.append(dadGt.pl(1))
              sb += '\t'
              sb.append(momGt.pl(1))
              sb += '\t'
              sb.tsvAppend(kidAdRatio)
              sb += '\t'
              sb.tsvAppend(dadAdRatio)
              sb += '\t'
              sb.tsvAppend(momAdRatio)
              sb += '\t'
              sb.append(kidDp)
              sb += '\t'
              sb.append(dadGt.dp)
              sb += '\t'
              sb.append(momGt.dp)
              sb += '\t'
              sb.tsvAppend(dpRatio)
              sb += '\t'
              sb.tsvAppend(pTrueDeNovo)

              additionalQueries.foreach { case (ec, fs) =>
                ec.set(0, v)
                ec.set(1, va)
                ec.set(2, localGlobal)
                ec.set(3, Annotation(arr(i, 0), annotationMap.value(kidId), kidId))
                ec.set(4, Annotation(arr(i, 1), annotationMap.value(dadId), dadId))
                ec.set(5, Annotation(arr(i, 2), annotationMap.value(momId), momId))
                fs.foreachBetween({ case (t, f) => sb.append(t.str(f())) })(sb += '\t')
              }

              sb.result()
            }
          }
      }
    }
  }
}
