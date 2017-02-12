package is.hail.methods

import is.hail.annotations.Annotation
import is.hail.variant.{Genotype, GenotypeBuilder, GenotypeStreamBuilder, Variant}
import is.hail.utils._

object SplitMulti {

  def splitGT(gt: Int, i: Int): Int = {
    val p = Genotype.gtPair(gt)
    (if (p.j == i) 1 else 0) +
      (if (p.k == i) 1 else 0)
  }

  def split(v: Variant,
    va: Annotation,
    it: Iterable[Genotype],
    propagateGQ: Boolean,
    compress: Boolean,
    isDosage: Boolean,
    keepStar: Boolean,
    insertSplitAnnots: (Annotation, Int, Boolean) => Annotation): Iterator[(Variant, (Annotation, Iterable[Genotype]))] = {

    if (v.isBiallelic)
      return Iterator((v, (insertSplitAnnots(va, 1, false), it)))

    val splitVariants = v.altAlleles.iterator.zipWithIndex
      .filter(keepStar || _._1.alt != "*")
      .map { case (aa, aai) =>
        (Variant(v.contig, v.start, v.ref, Array(aa)).minrep, aai + 1)
      }.toArray

    val splitGenotypeBuilders = splitVariants.map { case (sv, _) => new GenotypeBuilder(sv.nAlleles, isDosage) }
    val splitGenotypeStreamBuilders = splitVariants.map { case (sv, _) => new GenotypeStreamBuilder(sv.nAlleles, isDosage, compress) }

    for (g <- it) {

      val gadsum = g.ad.map(gadx => (gadx, gadx.sum))

      // svj corresponds to the ith allele of v
      for (((svj, i), j) <- splitVariants.iterator.zipWithIndex) {
        val gb = splitGenotypeBuilders(j)
        gb.clear()

        if (!isDosage) {
          g.gt.foreach { ggtx =>
            val gtx = splitGT(ggtx, i)
            gb.setGT(gtx)

            val p = Genotype.gtPair(ggtx)
            if (gtx != p.nNonRefAlleles)
              gb.setFakeRef()
          }

          gadsum.foreach { case (gadx, sum) =>
            // what bcftools does
            // Array(gadx(0), gadx(i))
            gb.setAD(Array(sum - gadx(i), gadx(i)))
          }

          g.dp.foreach { dpx => gb.setDP(dpx) }

          if (propagateGQ)
            g.gq.foreach { gqx => gb.setGQ(gqx) }

          g.pl.foreach { gplx =>
            val plx = gplx.iterator.zipWithIndex
              .map { case (p, k) => (splitGT(k, i), p) }
              .reduceByKeyToArray(3, Int.MaxValue)(_ min _)
            gb.setPX(plx)

            if (!propagateGQ) {
              val gq = Genotype.gqFromPL(plx)
              gb.setGQ(gq)
            }
          }
        } else {
          val newpx = g.dosage.map { gdx =>
            val dx = gdx.iterator.zipWithIndex
              .map { case (d, k) => (splitGT(k, i), d) }
              .reduceByKeyToArray(3, 0.0)(_ + _)

            val px = Genotype.weightsToLinear(dx)
            gb.setPX(px)
            px
          }

          val newgt = newpx
            .flatMap { px => Genotype.gtFromLinear(px) }
            .getOrElse(-1)

          if (newgt != -1)
            gb.setGT(newgt)

          g.gt.foreach { gtx =>
            val p = Genotype.gtPair(gtx)
            if (newgt != p.nNonRefAlleles && newgt != -1)
              gb.setFakeRef()
          }
        }

        splitGenotypeStreamBuilders(j).write(gb)
      }
    }

    splitVariants.iterator
      .zip(splitGenotypeStreamBuilders.iterator)
      .map { case ((v, ind), gsb) =>
        (v, (insertSplitAnnots(va, ind, true), gsb.result()))
      }
  }

  def splitNumber(str: String): String =
    if (str == "A" || str == "R" || str == "G")
      "."
    else str
}
