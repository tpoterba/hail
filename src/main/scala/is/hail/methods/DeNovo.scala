package is.hail.methods

import is.hail.keytable.KeyTable
import is.hail.variant._
import is.hail.expr._
import is.hail.utils._
import is.hail.annotations._

import scala.collection.mutable

object DeNovo {

  def keytableDefaultFields: Array[(String, Type)] = Array(
    "v" -> TVariant,
    "Proband_ID" -> TString,
    "Father_ID" -> TString,
    "Mother_ID" -> TString,
    "Proband_is_female" -> TBoolean,
    "Proband_is_case" -> TBoolean,
    "Validation_likelihood" -> TDouble,
    "Proband_gt" -> TGenotype,
    "Mother_gt" -> TGenotype,
    "Father_gt" -> TGenotype,
    "DP_Ratio" -> TDouble,
    "P_de_novo" -> TDouble)


  val PRIOR = 1.0 / 30000000

  val MIN_PRIOR = 100.0 / 30000000

  val dpCutoff = .10

  val HEADER = Array("Chr", "Pos", "Ref", "Alt", "Proband_ID", "Father_ID",
    "Mother_ID", "Proband_Sex", "Proband_AffectedStatus", "Validation_likelihood", "Proband_PL_AA",
    "Father_PL_AB", "Mother_PL_AB", "Proband_AD_Ratio", "Father_AD_Ratio",
    "Mother_AD_Ratio", "DP_Proband", "DP_Father", "DP_Mother", "DP_Ratio",
    "P_de_novo")

  def call(vds: VariantDataset, famFile: String,
    referenceAFExpr: String,
    extraFieldsExpr: Option[String] = None,
    minGQ: Int = 20,
    minimumPDeNovo: Double = 0.05,
    maxParentAB: Double = 0.05,
    minChildAB: Double = 0.20,
    minDepthRatio: Double = 0.10): KeyTable = {
    require(vds.wasSplit)

    val ped = Pedigree.read(famFile, vds.hadoopConf, vds.sampleIds)

    val (popFrequencyT, popFrequencyF) = vds.queryVA(referenceAFExpr)
    if (popFrequencyT != TDouble)
      fatal(s"population frequency should be a Double, but got `$popFrequencyT'")

    val popFreqQuery: (Annotation) => Option[Double] =
      (a: Annotation) => Option(popFrequencyF(a)).map(_.asInstanceOf[Double])

    val additionalOutput = extraFieldsExpr.map { cond =>
      val symTab = Map(
        "v" -> (0, TVariant),
        "va" -> (1, vds.vaSignature),
        "global" -> (2, vds.globalSignature),
        "proband" -> (3, TStruct(("g", TGenotype), ("annot", vds.saSignature), ("id", TString))),
        "mother" -> (4, TStruct(("g", TGenotype), ("annot", vds.saSignature), ("id", TString))),
        "father" -> (5, TStruct(("g", TGenotype), ("annot", vds.saSignature), ("id", TString)))
      )


      val ec = EvalContext(symTab)

      val (names, types, fs) = Parser.parseNamedExprs(cond, ec)
      (ec, names.zip(types), fs)
    }

    val schema = TStruct(keytableDefaultFields ++ additionalOutput.map(o => o._2).getOrElse(Array.empty): _*)
    val nFields = schema.size
    val nDefaultFields = keytableDefaultFields.length

    val trios = ped.completeTrios.filter(_.sex.isDefined)
    val nSamplesDiscarded = ped.trios.length - trios.length
    val nTrios = trios.size

    info(s"Calling de novo events for $nTrios trios")

    if (nSamplesDiscarded > 0)
      warn(s"$nSamplesDiscarded ${ plural(nSamplesDiscarded, "sample") } discarded from .fam: missing from data set.")
    val sampleTrioRoles = mutable.Map.empty[String, List[(Int, Int)]]

    // need a map from Sample position(int) to (int, int)
    trios.zipWithIndex.foreach { case (t, ti) =>
      sampleTrioRoles += (t.kid -> ((ti, 0) :: sampleTrioRoles.getOrElse(t.kid, List.empty[(Int, Int)])))
      sampleTrioRoles += (t.dad -> ((ti, 1) :: sampleTrioRoles.getOrElse(t.dad, List.empty[(Int, Int)])))
      sampleTrioRoles += (t.mom -> ((ti, 2) :: sampleTrioRoles.getOrElse(t.mom, List.empty[(Int, Int)])))
    }

    val idMapping = vds.sampleIds.zipWithIndex.toMap

    val sc = vds.sparkContext
    val trioIndexBc = sc.broadcast(trios.map(t => (idMapping(t.kid), idMapping(t.dad), idMapping(t.mom))))
    val sampleTrioRolesBc = sc.broadcast(vds.sampleIds.map(sampleTrioRoles.getOrElse(_, Nil)).toArray)
    val triosBc = sc.broadcast(trios)
    val trioSexBc = sc.broadcast(trios.map(_.sex.get))

    val localGlobal = vds.globalAnnotation
    val localAnnotationsBc = vds.sampleAnnotationsBc

    val rdd = vds.rdd.mapPartitions { iter =>
      val arr = MultiArray2.fill[CompleteGenotype](trios.length, 3)(null)

      iter.flatMap { case (v, (va, gs)) =>
        var i = 0

        var totalAlleles = 0
        var nAltAlleles = 0

        var ii = 0
        while (ii < nTrios) {
          var jj = 0
          while (jj < 3) {
            arr.update(ii, jj, null)
            jj += 1
          }
          ii += 1
        }

        gs.foreach { g =>
          g.toCompleteGenotype.foreach { cg =>
            val roles = sampleTrioRolesBc.value(i)
            roles.foreach { case (ri, ci) => arr.update(ri, ci, cg) }

            nAltAlleles += cg.gt
            totalAlleles += 2
          }
          i += 1
        }

        // correct for observed genotype
        val computedFrequency = (nAltAlleles.toDouble - 1) / totalAlleles.toDouble

        val popFrequency = popFreqQuery(va).getOrElse(0d)
        if (popFrequency < 0 || popFrequency > 1)
          fatal(
            s"""invalid population frequency value `$popFrequency' for variant $v
                  Population prior must fall between 0 and 1.""".stripMargin)

        val frequency = math.max(math.max(computedFrequency, popFrequency), MIN_PRIOR)

        (0 until nTrios).flatMap { i =>
          val kid = arr(i, 0)
          val dad = arr(i, 1)
          val mom = arr(i, 2)
//          if (kid != null && kid.gt == 1 &&
//          dad != null && dad.gt == 0 &&
//          mom != null && mom.gt == 0)
//            println(s"right genotypes: $kid / $dad / $mom")

//          if (kid == "FIpEHPEo3" && v.start == 486)
//            println(dad.ad(1).toDouble / (dad.ad(0) + dad.ad(1)) > maxParentAB,
//              mom.ad(1).toDouble / (mom.ad(0) + mom.ad(1)) > maxParentAB)

          if (kid == null || kid.gt != 1 ||
            dad == null || dad.gt != 0 ||
            mom == null || mom.gt != 0 ||
            kid.pl(0) <= minGQ ||
            (kid.ad(0) == 0) && (kid.ad(1) == 0) ||
            (dad.ad(0) == 0) && (dad.ad(1) == 0) ||
            (mom.ad(0) == 0) && (mom.ad(1) == 0) ||
            kid.ad(0).toDouble / (kid.ad(0) + kid.ad(1)) <= minChildAB ||
            dad.ad(1).toDouble / (dad.ad(0) + dad.ad(1)) >= maxParentAB ||
            mom.ad(1).toDouble / (mom.ad(0) + mom.ad(1)) >= maxParentAB ||
            kid.dp.toDouble / (mom.dp + dad.dp) < minDepthRatio)
            None
          else {

//            println(s"found candidate: ${ kid } / $dad / $mom")
            //            // fixme precomputed
            //
            //            if (v.start == 171493)
            //              println(s"at variant $v, vcfFreq = ${computedFrequency}, espFreq = ${popFrequency}")

            val kidP = kid.pl.map(x => math.pow(10, -x / 10d))
            val dadP = dad.pl.map(x => math.pow(10, -x / 10d))
            val momP = mom.pl.map(x => math.pow(10, -x / 10d))

            val kidSum = kidP.sum
            val dadSum = dadP.sum
            val momSum = momP.sum

            (0 until 3).foreach { i =>
              kidP(i) = kidP(i) / kidSum
              dadP(i) = dadP(i) / dadSum
              momP(i) = momP(i) / momSum
            }

            val pDeNovoData = dadP(0) * momP(0) * kidP(1) * PRIOR

            val pDataOneHet = (dadP(1) * momP(0) + dadP(0) * momP(1)) * kidP(1)
            val pOneParentHet = 1 - math.pow(1 - frequency, 4)
            val pMissedHetInParent = pDataOneHet * pOneParentHet

            val pTrueDeNovo = pDeNovoData / (pDeNovoData + pMissedHetInParent)

            val kidAdRatio = kid.ad(1).toDouble / (kid.ad(0) + kid.ad(1))

            val kidDp = kid.dp
            val dpRatio = kidDp.toDouble / (mom.dp + dad.dp)

            // Below is the core calling algorithm
            val genotypeAnnotation = if (pTrueDeNovo < minimumPDeNovo)
              None
            else if (v.altAllele.isIndel) {
              if ((pTrueDeNovo > 0.99) && (kidAdRatio > 0.3) && (nAltAlleles == 1))
                Some("HIGH_indel")
              else if ((pTrueDeNovo > 0.5) && (kidAdRatio > 0.3) && (nAltAlleles <= 5))
                Some("MEDIUM_indel")
              else if ((pTrueDeNovo > 0.05) && (kidAdRatio > 0.20))
                Some("LOW_indel")
              else None
            } else {
              if ((pTrueDeNovo > 0.99) && (kidAdRatio > 0.3) && (dpRatio > 0.2) ||
                ((pTrueDeNovo > 0.99) && (kidAdRatio > 0.3) && (nAltAlleles == 1)) ||
                ((pTrueDeNovo > 0.5) && (kidAdRatio >= 0.3) && (nAltAlleles < 10) && (kidDp >= 10))
              )
                Some("HIGH_SNV")
              else if ((pTrueDeNovo > 0.5) && (kidAdRatio > 0.3) ||
                ((pTrueDeNovo > 0.5) && (kidAdRatio > minDepthRatio) && (nAltAlleles == 1))
              )
                Some("MEDIUM_SNV")
              else if ((pTrueDeNovo > 0.05) && (kidAdRatio > 0.20))
                Some("LOW_SNV")
              else None
            }

            genotypeAnnotation.map { str =>

              val defaults: Array[Annotation] = Array(
                v,
                triosBc.value(i).kid,
                triosBc.value(i).dad,
                triosBc.value(i).mom,
                triosBc.value(i).sex.map(s => s == Sex.Female).orNull,
                triosBc.value(i).pheno.map(p => p == Phenotype.Case).orNull,
                str,
                kid.toGenotype,
                dad.toGenotype,
                mom.toGenotype,
                dpRatio,
                pTrueDeNovo)

              val fullRow = additionalOutput match {
                case Some((ec, _, fs)) =>
                  val t = triosBc.value(i)
                  val kidId = t.kid
                  val dadId = t.dad
                  val momId = t.mom

                  val (kidIndex, dadIndex, momIndex) = trioIndexBc.value(i)

                  ec.set(0, v)
                  ec.set(1, va)
                  ec.set(2, localGlobal)
                  ec.set(3, Annotation(arr(i, 0), localAnnotationsBc.value(kidIndex)))
                  ec.set(4, Annotation(arr(i, 1), localAnnotationsBc.value(dadIndex)))
                  ec.set(5, Annotation(arr(i, 2), localAnnotationsBc.value(momIndex)))

                  val results = fs()
                  val combined = defaults ++ results
                  assert(combined.length == nFields)
                  combined
                case None => defaults
              }
              Annotation.fromSeq(fullRow)
            }
          }
        }.iterator
      }
    }.cache()

    KeyTable(vds.hc, rdd, schema, Array.empty)
  }
}