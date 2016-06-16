package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.check.Gen
import org.broadinstitute.hail.check.Prop._
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.driver.{FilterExportTriosSamocha, State}
import org.broadinstitute.hail.expr.TDouble
import org.testng.annotations.Test

import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._



class DeNovoSuite extends SparkSuite {

  @Test def test() {
    val dnCallerPath = "/Users/tpoterba/Downloads/de_novo_finder_3.py"

//    s"python $dnCallerPath -h " !
    val pedFile = tmpDir.createTempFile("pedigree", extension = ".fam")
    val espFile = tmpDir.createTempFile("esp", extension = ".txt")

    val g = for (vds: VariantDataset <- VariantSampleMatrix.gen(sc,
      VSMSubGens(vGen = Variant.genPlinkCompatible, tGen = Genotype.genRealistic(_)));
    ped <- Pedigree.gen(vds.sampleIds)) yield (vds, ped, vds
      .variants
      .collect()
      .map(v => (v, Gen.choose(0.0, 1.0).sample())))

    val p = forAll(g)({ case (vds, ped, vMap) =>
        val ped = Pedigree
          .gen(vds.sampleIds)
          .sample()
          .write(pedFile, hadoopConf)

      val esp = writeTextFile(espFile, hadoopConf) { out =>
        vMap.foreach { case (v, maf) =>
          out.write(s"${v.contig} ${v.start} ${v.ref} ${v.alt}")
        }
      }
        true
    })

    val (vds, ped, map) = g.sample()

    val f1 = tmpDir.createTempFile("fam", ".fam")
    ped.write(f1, hadoopConf)
    var s = State(sc, sqlContext, vds.annotateVariants(sc.parallelize(map),TDouble, List("pop")).copy(wasSplit = true))
    FilterExportTriosSamocha.run(s, Array("--fam", f1, "--pop-freq", "va.pop", "-o", "/tmp/out.txt", "-e", "dadgt = father.geno, dadgt = proband.geno, dadgt = mother.geno"))
    readFile("/tmp/out.txt", hadoopConf) { in => (Source.fromInputStream(in).getLines().flatMap(x => x.split("\t")).foreach(println))}
//
//    s"$dnCallerPath " !
  }
}
