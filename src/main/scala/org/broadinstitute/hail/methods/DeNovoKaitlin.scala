package org.broadinstitute.hail.methods

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.utils.MultiArray2


import scala.collection.mutable

object DeNovoKaitlin {

  val PRIOR = 1.0 / 30000000

  val MIN_PRIOR = 100.0 / 30000000

  val dpCutoff = .10

  def validGt(g: Genotype): Boolean = g.pl.isDefined && g.ad.isDefined && g.gq.isDefined && g.dp.isDefined

  def validParent(g: Genotype): Boolean = g.isHomRef && validGt(g)

  def validProband(g: Genotype): Boolean = g.isHet && validGt(g)

  def apply(vds: VariantDataset, preTrios: Array[CompleteTrio], f: (Variant, Annotation) => Option[Double]): VariantDataset = {
    val trios = preTrios.filter(_.sex.isDefined)
    val nSamplesDiscarded = preTrios.size - trios.size

    if (nSamplesDiscarded > 0)
      warn(s"$nSamplesDiscarded ${plural(nSamplesDiscarded, "sample")} discarded from .fam: missing from variant data set.")
    val sampleTrioRoles = mutable.Map.empty[String, List[(Int, Int)]]


    // need a map from Sample position(int) to (int, int)
    trios.zipWithIndex.foreach { case (t, ti) =>
      sampleTrioRoles += (t.kid -> ((ti, 0) :: sampleTrioRoles.getOrElse(t.kid, List.empty[(Int, Int)])))
      sampleTrioRoles += (t.dad -> ((ti, 1) :: sampleTrioRoles.getOrElse(t.dad, List.empty[(Int, Int)])))
      sampleTrioRoles += (t.mom -> ((ti, 2) :: sampleTrioRoles.getOrElse(t.mom, List.empty[(Int, Int)])))
    }

    val sc = vds.sparkContext

    val sampleTrioRolesBc = sc.broadcast(sampleTrioRoles.toMap)
    val triosBc = sc.broadcast(trios)
    val trioSexBc = sc.broadcast(trios.map(_.sex.get))

    val arrBc = sc.broadcast()

    val localIdsBc = vds.sampleIdsBc
    val res = vds.rdd.mapPartitions { iter =>
      val arr = MultiArray2.fill[Genotype](trios.length, 3)(Genotype())

      iter.map { case (v, va, gs) =>
        val (nCalled, nAltAlleles) = (gs, localIdsBc.value).zipped.map {
          case (gt, id) =>
            sampleTrioRolesBc.value.get(id)
              .foreach { l =>
                l.foreach { case (i, j) =>
                  arr.update(i, j, gt)
                }
              }
            gt.nNonRefAlleles.map(o => (1, o)).getOrElse((0, 0))
        }.sum

        val datasetFrequency = nAltAlleles.toDouble / (nCalled * 2)

        val popFrequency = f(v, va).getOrElse(0d)

        val frequency = math.max(math.max(datasetFrequency, popFrequency), MIN_PRIOR)

        trios.indices
          .filter { i => validProband(arr(i, 0)) && validParent(arr(i, 1)) && validParent(arr(i, 2)) }
          .flatMap { i =>
            val kidGt = arr(i, 0)
            val dadGt = arr(i, 1)
            val momGt = arr(i, 2)

            val kidP = kidGt.pl.get.map(x => math.pow(10, -x / 10))
            val dadP = dadGt.pl.get.map(x => math.pow(10, -x / 10))
            val momP = momGt.pl.get.map(x => math.pow(10, -x / 10))

            val pDeNovoData = dadP(0) * momP(0) * kidP(0) * PRIOR

            val pDataOneHet = (dadP(1) * momP(0) + dadP(0) * momP(1)) * kidP(1)
            val pOneParentHet = 1 - math.pow(1 - frequency, 4)
            val pMhipData = pDataOneHet * pOneParentHet

            val metric = pDeNovoData / (pDeNovoData + pMhipData)

            val momAdRatio = {
              val ad = momGt.ad.get
              ad(1) / ad.sum
            }

            val dadAdRatio = {
              val ad = dadGt.ad.get
              ad(1) / ad.sum
            }

            val kidAdRatio = {
              val ad = kidGt.ad.get
              ad(1) / ad.sum
            }

          val dpRatio = kidGt.dp.get.toDouble / (momGt.dp.get + dadGt.dp.get)

            Some(metric)
          }
      }
    }
  }

}
