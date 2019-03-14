import unittest

import hail as hl
from ..helpers import *

setUpModule = startTestHailContext
tearDownModule = stopTestHailContext


class Tests(unittest.TestCase):
    def test_rename_duplicates(self):
        mt = hl.utils.range_matrix_table(5, 5)

        assert hl.rename_duplicates(
            mt.key_cols_by(s=hl.str(mt.col_idx))
        ).unique_id.collect() == ['0', '1', '2', '3', '4']

        assert hl.rename_duplicates(
            mt.key_cols_by(s='0')
        ).unique_id.collect() == ['0', '0_1', '0_2', '0_3', '0_4']

        assert hl.rename_duplicates(
            mt.key_cols_by(s=hl.literal(['0', '0_1', '0', '0_2', '0'])[mt.col_idx])
        ).unique_id.collect() == ['0', '0_1', '0_2', '0_2_1', '0_3']

        assert hl.rename_duplicates(
            mt.key_cols_by(s=hl.str(mt.col_idx)),
            'foo'
        )['foo'].dtype == hl.tstr

    def test_annotate_intervals(self):
        ds = get_dataset()

        bed1 = hl.import_bed(resource('example1.bed'), reference_genome='GRCh37')
        bed2 = hl.import_bed(resource('example2.bed'), reference_genome='GRCh37')
        bed3 = hl.import_bed(resource('example3.bed'), reference_genome='GRCh37')
        self.assertTrue(list(bed2.key.dtype) == ['interval'])
        self.assertTrue(list(bed2.row.dtype) == ['interval', 'target'])

        interval_list1 = hl.import_locus_intervals(resource('exampleAnnotation1.interval_list'))
        interval_list2 = hl.import_locus_intervals(resource('exampleAnnotation2.interval_list'))
        self.assertTrue(list(interval_list2.key.dtype) == ['interval'])
        self.assertTrue(list(interval_list2.row.dtype) == ['interval', 'target'])

        ann = ds.annotate_rows(in_interval=bed1[ds.locus]).rows()
        self.assertTrue(ann.all((ann.locus.position <= 14000000) |
                                (ann.locus.position >= 17000000) |
                                (hl.is_missing(ann.in_interval))))

        for bed in [bed2, bed3]:
            ann = ds.annotate_rows(target=bed[ds.locus].target).rows()
            expr = (hl.case()
                    .when(ann.locus.position <= 14000000, ann.target == 'gene1')
                    .when(ann.locus.position >= 17000000, ann.target == 'gene2')
                    .default(ann.target == hl.null(hl.tstr)))
            self.assertTrue(ann.all(expr))

        self.assertTrue(ds.annotate_rows(in_interval=interval_list1[ds.locus]).rows()
                        ._same(ds.annotate_rows(in_interval=bed1[ds.locus]).rows()))

        self.assertTrue(ds.annotate_rows(target=interval_list2[ds.locus].target).rows()
                        ._same(ds.annotate_rows(target=bed2[ds.locus].target).rows()))

    @skip_unless_spark_backend()
    def test_maximal_independent_set(self):
        # prefer to remove nodes with higher index
        t = hl.utils.range_table(10)
        graph = t.select(i=hl.int64(t.idx), j=hl.int64(t.idx + 10), bad_type=hl.float32(t.idx))

        mis_table = hl.maximal_independent_set(graph.i, graph.j, True, lambda l, r: l - r)
        mis = [row['node'] for row in mis_table.collect()]
        self.assertEqual(sorted(mis), list(range(0, 10)))
        self.assertEqual(mis_table.row.dtype, hl.tstruct(node=hl.tint64))
        self.assertEqual(mis_table.key.dtype, hl.tstruct(node=hl.tint64))

        self.assertRaises(ValueError, lambda: hl.maximal_independent_set(graph.i, graph.bad_type, True))
        self.assertRaises(ValueError, lambda: hl.maximal_independent_set(graph.i, hl.utils.range_table(10).idx, True))
        self.assertRaises(ValueError, lambda: hl.maximal_independent_set(hl.literal(1), hl.literal(2), True))

    @skip_unless_spark_backend()
    def test_maximal_independent_set2(self):
        edges = [(0, 4), (0, 1), (0, 2), (1, 5), (1, 3), (2, 3), (2, 6),
                 (3, 7), (4, 5), (4, 6), (5, 7), (6, 7)]
        edges = [{"i": l, "j": r} for l, r in edges]

        t = hl.Table.parallelize(edges, hl.tstruct(i=hl.tint64, j=hl.tint64))
        mis_t = hl.maximal_independent_set(t.i, t.j)
        self.assertTrue(mis_t.row.dtype == hl.tstruct(node=hl.tint64) and
                        mis_t.globals.dtype == hl.tstruct())

        mis = set([row.node for row in mis_t.collect()])
        maximal_indep_sets = [{0, 6, 5, 3}, {1, 4, 7, 2}]
        non_maximal_indep_sets = [{0, 7}, {6, 1}]
        self.assertTrue(mis in non_maximal_indep_sets or mis in maximal_indep_sets)

    @skip_unless_spark_backend()
    def test_maximal_independent_set3(self):
        is_case = {"A", "C", "E", "G", "H"}
        edges = [("A", "B"), ("C", "D"), ("E", "F"), ("G", "H")]
        edges = [{"i": {"id": l, "is_case": l in is_case},
                  "j": {"id": r, "is_case": r in is_case}} for l, r in edges]

        t = hl.Table.parallelize(edges, hl.tstruct(i=hl.tstruct(id=hl.tstr, is_case=hl.tbool),
                                                   j=hl.tstruct(id=hl.tstr, is_case=hl.tbool)))

        tiebreaker = lambda l, r: (hl.case()
                                   .when(l.is_case & (~r.is_case), -1)
                                   .when(~(l.is_case) & r.is_case, 1)
                                   .default(0))

        mis = hl.maximal_independent_set(t.i, t.j, tie_breaker=tiebreaker)

        expected_sets = [{"A", "C", "E", "G"}, {"A", "C", "E", "H"}]

        self.assertTrue(mis.all(mis.node.is_case))
        self.assertTrue(set([row.id for row in mis.select(mis.node.id).collect()]) in expected_sets)

    @skip_unless_spark_backend()
    def test_maximal_independent_set_types(self):
        ht = hl.utils.range_table(10)
        ht = ht.annotate(i=hl.struct(a='1', b=hl.rand_norm(0, 1)),
                         j=hl.struct(a='2', b=hl.rand_norm(0, 1)))
        ht = ht.annotate(ii=hl.struct(id=ht.i, rank=hl.rand_norm(0, 1)),
                         jj=hl.struct(id=ht.j, rank=hl.rand_norm(0, 1)))
        hl.maximal_independent_set(ht.ii, ht.jj).count()

    def test_matrix_filter_intervals(self):
        ds = hl.import_vcf(resource('sample.vcf'), min_partitions=20)

        self.assertEqual(
            hl.filter_intervals(ds, [hl.parse_locus_interval('20:10639222-10644705')]).count_rows(), 3)

        intervals = [hl.parse_locus_interval('20:10639222-10644700'),
                     hl.parse_locus_interval('20:10644700-10644705')]
        self.assertEqual(hl.filter_intervals(ds, intervals).count_rows(), 3)

        intervals = hl.array([hl.parse_locus_interval('20:10639222-10644700'),
                              hl.parse_locus_interval('20:10644700-10644705')])
        self.assertEqual(hl.filter_intervals(ds, intervals).count_rows(), 3)

        intervals = hl.array([hl.eval(hl.parse_locus_interval('20:10639222-10644700')),
                              hl.parse_locus_interval('20:10644700-10644705')])
        self.assertEqual(hl.filter_intervals(ds, intervals).count_rows(), 3)

        intervals = [hl.eval(hl.parse_locus_interval('[20:10019093-10026348]')),
                     hl.eval(hl.parse_locus_interval('[20:17705793-17716416]'))]
        self.assertEqual(hl.filter_intervals(ds, intervals).count_rows(), 4)

    def test_table_filter_intervals(self):
        ds = hl.import_vcf(resource('sample.vcf'), min_partitions=20).rows()

        self.assertEqual(
            hl.filter_intervals(ds, [hl.parse_locus_interval('20:10639222-10644705')]).count(), 3)

        intervals = [hl.parse_locus_interval('20:10639222-10644700'),
                     hl.parse_locus_interval('20:10644700-10644705')]
        self.assertEqual(hl.filter_intervals(ds, intervals).count(), 3)

        intervals = hl.array([hl.parse_locus_interval('20:10639222-10644700'),
                              hl.parse_locus_interval('20:10644700-10644705')])
        self.assertEqual(hl.filter_intervals(ds, intervals).count(), 3)

        intervals = hl.array([hl.eval(hl.parse_locus_interval('20:10639222-10644700')),
                              hl.parse_locus_interval('20:10644700-10644705')])
        self.assertEqual(hl.filter_intervals(ds, intervals).count(), 3)

        intervals = [hl.eval(hl.parse_locus_interval('[20:10019093-10026348]')),
                     hl.eval(hl.parse_locus_interval('[20:17705793-17716416]'))]
        self.assertEqual(hl.filter_intervals(ds, intervals).count(), 4)

    def test_filter_intervals_compound_key(self):
        ds = hl.import_vcf(resource('sample.vcf'), min_partitions=20)
        ds = (ds.annotate_rows(variant=hl.struct(locus=ds.locus, alleles=ds.alleles))
              .key_rows_by('locus', 'alleles'))

        intervals = [hl.Interval(hl.Struct(locus=hl.Locus('20', 10639222), alleles=['A', 'T']),
                                 hl.Struct(locus=hl.Locus('20', 10644700), alleles=['A', 'T']))]
        self.assertEqual(hl.filter_intervals(ds, intervals).count_rows(), 3)

    def test_window_by_locus(self):
        mt = hl.utils.range_matrix_table(100, 2, n_partitions=10)
        mt = mt.annotate_rows(locus=hl.locus('1', mt.row_idx + 1))
        mt = mt.key_rows_by('locus')
        mt = mt.annotate_entries(e_row_idx=mt.row_idx, e_col_idx=mt.col_idx)
        mt = hl.window_by_locus(mt, 5).cache()

        self.assertEqual(mt.count_rows(), 100)

        rows = mt.rows()
        self.assertTrue(rows.all((rows.row_idx < 5) | (rows.prev_rows.length() == 5)))
        self.assertTrue(rows.all(hl.all(lambda x: (rows.row_idx - 1 - x[0]) == x[1].row_idx,
                                        hl.zip_with_index(rows.prev_rows))))

        entries = mt.entries()
        self.assertTrue(entries.all(hl.all(lambda x: x.e_col_idx == entries.col_idx, entries.prev_entries)))
        self.assertTrue(entries.all(hl.all(lambda x: entries.row_idx - 1 - x[0] == x[1].e_row_idx,
                                           hl.zip_with_index(entries.prev_entries))))

    def test_summarize_variants(self):
        mt = hl.utils.range_matrix_table(3, 3)
        variants = hl.literal({0: hl.Struct(locus=hl.Locus('1', 1), alleles=['A', 'T', 'C']),
                               1: hl.Struct(locus=hl.Locus('2', 1), alleles=['A', 'AT', '@']),
                               2: hl.Struct(locus=hl.Locus('2', 1), alleles=['AC', 'GT'])})
        mt = mt.annotate_rows(**variants[mt.row_idx]).key_rows_by('locus', 'alleles')
        r = hl.summarize_variants(mt, show=False)
        self.assertEqual(r.n_variants, 3)
        self.assertEqual(r.contigs, {'1': 1, '2': 2})
        self.assertEqual(r.allele_types, {'SNP': 2, 'MNP': 1, 'Unknown': 1, 'Insertion': 1})
        self.assertEqual(r.allele_counts, {2: 1, 3: 2})

    def test_verify_biallelic(self):
        mt = hl.import_vcf(resource('sample2.vcf'))  # has multiallelics
        with self.assertRaises(hl.utils.FatalError):
            hl.methods.misc.require_biallelic(mt, '')._force_count_rows()
