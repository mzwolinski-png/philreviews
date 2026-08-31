"""Unit tests for the Springer 'Review of: Author, Title, <pub info>' parser
(scripts/springer_scan.parse_review_citation) — no DB / network."""
import os, sys, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import springer_scan as ss


class ParseReviewCitation(unittest.TestCase):
    def test_single_author(self):
        t, af, al = ss.parse_review_citation(
            "Review of: Michael Wachtel, Viacheslav Ivanov: A Symbolist Life, "
            "New York, Columbia University Press, 2025, 736 pp., ISBN 9780231218375, $45.00")
        self.assertEqual((af, al), ("Michael", "Wachtel"))
        self.assertEqual(t, "Viacheslav Ivanov: A Symbolist Life")

    def test_title_with_internal_commas(self):
        # commas inside the title must be preserved, not treated as author breaks
        t, af, al = ss.parse_review_citation(
            "Review of: Vera Mironova, Criminals, Nazis, and Islamists: Competition for "
            "Power in Former Soviet Union Prisons, New York, Oxford University Press, 2023, "
            "309 pages, Paperback: ISBN 978-0197645666, £20.99")
        self.assertEqual((af, al), ("Vera", "Mironova"))
        self.assertEqual(t, "Criminals, Nazis, and Islamists: Competition for Power in "
                            "Former Soviet Union Prisons")

    def test_multi_author_and(self):
        t, af, al = ss.parse_review_citation(
            "Review of: Tatiana Rezvykh and Teresa Obolevich, The Correspondence, "
            "Izdatel'stvo THEOS-LOGOS/ISBN 978-83-947280-7-6")
        self.assertEqual((af, al), ("Tatiana Rezvykh and Teresa", "Obolevich"))
        self.assertEqual(t, "The Correspondence")

    def test_editors_form(self):
        t, af, al = ss.parse_review_citation(
            "Review of: E. Takho-Godi, A. Shishkin (Editors and compilers), "
            "Vjacheslav Ivanov. Vyp. 4, Moscow, Vodoley Publ., 2024, 852 pp., "
            "ISBN 978-5-9208-0782-3, 2527")
        self.assertEqual((af, al), ("E. Takho-Godi, A.", "Shishkin"))
        self.assertEqual(t, "Vjacheslav Ivanov. Vyp. 4")

    def test_ed_by_and_stray_pageid_stripped(self):
        t, af, al = ss.parse_review_citation(
            "Review of: Vladimir Solovyov, Materials and Studies: Epoch, People, Ideas, "
            "ed. by V. V. Sidorin, Moscow, Center for Humanitarian Initiatives Publ., "
            "2024, 516 p., ISBN 978-5-98712-482-6.    1416")
        self.assertEqual((af, al), ("Vladimir", "Solovyov"))
        self.assertEqual(t, "Materials and Studies: Epoch, People, Ideas")

    def test_no_colon_prefix(self):
        t, af, al = ss.parse_review_citation(
            "Review of Ani Kokobobo, Leo Tolstoy: the power of dissent, Boston, "
            "Academic Studies Press, 2025, 244 pages, hardback, ISBN 9798887197326")
        self.assertEqual((af, al), ("Ani", "Kokobobo"))
        self.assertEqual(t, "Leo Tolstoy: the power of dissent")

    def test_unattributed_edited_volume(self):
        # no author in the string -> author empty, title still cleaned
        t, af, al = ss.parse_review_citation(
            "Review of: Poles, Polonia, and the Quest for Liberty, Routledge, "
            "New York, 2025, 178 pages, hardcover, ISBN 978-1-032-97644-0, €175.00")
        self.assertEqual((af, al), ("", ""))
        self.assertEqual(t, "Poles, Polonia, and the Quest for Liberty")

    def test_non_review_title_passthrough(self):
        # ordinary titles are not 'Review of' citations -> None (leave untouched)
        self.assertIsNone(ss.parse_review_citation("Ideology and Meaning-Making under the Putin Regime"))
        self.assertIsNone(ss.parse_review_citation("Book Review"))
        self.assertIsNone(ss.parse_review_citation(""))


if __name__ == "__main__":
    unittest.main()


class EssayTitledReview(unittest.TestCase):
    """HPLS essay-style reviews: '<essay title>: on <Author>'s <Book>'."""
    BASE = {"genre": ["Book Review"], "url": [], "creators": [],
            "publicationDate": "2026-01-01", "doi": "10.1007/x"}

    def test_extracts_author_and_book(self):
        from springer_scan import parse_springer_record
        r = parse_springer_record({**self.BASE, "title":
            "Beyond anthropocentrism in comparative thanatology: "
            "on Susana Monsó’s Playing Possum"}, "HPLS")
        self.assertEqual(r["book_title"], "Playing Possum")
        self.assertEqual(r["book_author_first_name"], "Susana")
        self.assertEqual(r["book_author_last_name"], "Monsó")

    def test_plain_possessive_title_not_split(self):
        from springer_scan import parse_springer_record
        r = parse_springer_record({**self.BASE, "title":
            "Notes on Darwin's finches and their ecology"}, "HPLS")
        self.assertEqual(r["book_title"],
                         "Notes on Darwin's finches and their ecology")
        self.assertEqual(r["book_author_last_name"], "")
