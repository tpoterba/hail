# Calling _de novo_ variation with Hail

Hail has implemented Kaitlin Samocha's _de novo_ calling pipeline internally.  We have replicated the core functionality of the program, but have extended it to take advantage of Hail's infrastructure and annotation systems.  

 - `filterexporttrios samocha`
 - `filterexporttrios expr`

_____

## Kaitlin Samocha's _de novo_ calling pipeline

**Command line options:**

Argument | Description
:-: | ---
`-o`, `--output` | Output filename **(Required)**
`--pop-freq` | Annotation expression for population allele frequency, e.g. `va.exac.AF` **(Required)**
`--fam` | Path to fam file containing sample pedigree **(Required)**
`-e`, `--export` | Export expression **(Optional)**

We have implemented Kaitlin Samocha's _de novo_ calling pipeline.  This module produces a text file containing putative _de novo_ calls and expected validation probability.  

Result field | Description
:-: | ---
Chr                     | Contig containing the given variant
Pos                     | Genomic index on the contig
rsID                    | dbSNP annotation (or "." for missing)
Ref                     | Reference allele
Alt                     | Alternate allele
Proband_ID              | sample ID in the VDS
Father_ID               | sample ID in the VDS
Mother_ID               | sample ID in the VDS
Proband_Sex             | Sex of proband in .fam file (or "NA")
Proband_AffectedStatus  | Phenotype of proband in .fam file (or "NA")
Validation_likelihood   | Expected probability of replication, e.g. `HIGH_INDEL` or `MEDIUM_SNV`
Proband_PL_AA           | Homozygous reference PL value for proband
Father_PL_AB            | Heterozygous PL value for father
Mother_PL_AB            | Heterozygous PL value for mother
Proband_AD_Ratio        | alt read ratio: `(alt reads) / (alt reads + reference reads)`
Father_AD_Ratio         | alt read ratio: `(alt reads) / (alt reads + reference reads)`
Mother_AD_Ratio         | alt read ratio: `(alt reads) / (alt reads + reference reads)`
DP_Proband              | depth value for proband
DP_Father               | depth value for father
DP_Mother               | depth value for mother
DP_Ratio                | depth ratio: `(proband depth) / (mother depth + father depth)`  
P_de_novo               | Probability of true de novo, given population prior and observed measurements

It is possible to add fields to this output by using the `--condition` command line argument.  This argument takes an export expression which names column names and provides computations.  For example, if one wanted to include a variant gene annotation and a sample ancestry annotation for the proband, that would be possible with the following:

```
    $ hail < ... > filterexporttrios samocha
        --fam < fam file >
        --pop-freq va.exac.AF
        -o < out >
        --export 'Gene = va.gene, Ancestry = proband.anno.ancestry, PC1 = proband.anno.pca.pc1, PC2 = proband.anno.pca.pc2'
```

See [below](#namespace) for the namespace of this expression.

**\< REFERENCE TO PAPER HERE >**

____

## Using expressions to filter and export trios

Along with the specific algorithm discovered to be effective by K. Samocha et al., we have exposed a more general interface for filtering and exporting trios using the Hail expression language.  

Argument | Description
:-: | ---
`-o`, `--output` | Output filename **(Required)**
`-f`, `--fam` | Path to fam file containing sample pedigree **(Required)**
`-c`, `--condition` | Boolean filter expression **(Required)**
`-e`, `--export` | Export expression **(Required)**
`--remove` | Exclude trios satisfying the filter condition **(Default: false)**

The namespace for the `condition` and `export` expressions is as follows:

<a name="namespace"></a>
Variable | Description
:-: | ---
`v`  | variant
`va` | variant annotation
`global` | global annotation
`proband.id` | sample id for proband
`father.id` | sample id for father
`mother.id` | sample id for mother
`proband.anno` | sample annotation for proband
`father.anno` | sample annotation for father
`mother.anno` | sample annotation for mother
`proband.geno` | proband genotype ([full field list here](Representation.md#genotype)) 
`father.geno` | father genotype ([full field list here](Representation.md#genotype)) 
`mother.geno` | mother genotype ([full field list here](Representation.md#genotype)) 

#### Example 

Perhaps we are interested in the specific flavor of mendelian violation where both parents are opposite homozygotes (one homozygous reference and one homozygous variant), and the child is not called heterozygous.  We can collect these examples into a small, easily-analyzed file with the following usage:

```
    $ hail < ... > filterexporttrios expr 
        -o out.tsv
        -f file.fam
        -c '((father.geno.isHomRef & mother.geno.isHomVar) | (father.geno.isHomVar & mother.geno.isHomRef)) && !proband.geno.isHet'
        -e 'Variant=v, proband = proband.id, pro_gt = proband.geno, mom_gt = mother.geno, dad_gt = father.geno'
```