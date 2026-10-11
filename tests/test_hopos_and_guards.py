"""The 2026-10-11 weekly run: rows the review page showed as fine that were not.

Two separate failures. HOPOS deposits "Authors, eds. <i>Title</i>. Publisher,
Year. Pp. N."; nothing parsed it, so 38 rows read "Michael Heidelberger and
Gregor Schiemann" by "The Significance". And the integrity check "fixed" two
correct titles by splitting their first words off as an author.
"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from crossref_parsing import parse_review_title  # noqa: E402
from integrity_check import (author_reads_as_a_title, looks_like_a_phrase,  # noqa: E402
                             title_is_only_names)

TAIL = " . Berlin: de Gruyter, 2009. Pp. viii+376. $109.00 (cloth)."


class ItalicCitation(unittest.TestCase):
    def parse(self, raw):
        r = parse_review_title(raw)
        return r["book_title"], r["book_author_first"], r["book_author_last"], r["is_edited_volume"]

    def test_editors_before_an_italic_title(self):
        self.assertEqual(
            self.parse("Michael Heidelberger and Gregor Schiemann, eds. <i>The Significance of the "
                       "Hypothetical in the Natural Sciences</i>" + TAIL),
            ("The Significance of the Hypothetical in the Natural Sciences",
             "Michael Heidelberger and Gregor", "Schiemann", True))

    def test_single_author_with_initials(self):
        t, f, l, ed = self.parse("A. Douglas Stone. <i>Einstein and the Quantum</i> . Princeton, NJ: "
                                 "Princeton University Press, 2013. Pp. 332.")
        self.assertEqual((t, f, l, ed), ("Einstein and the Quantum", "A. Douglas", "Stone", False))

    def test_three_editors_one_with_stacked_initials(self):
        t, f, l, _ = self.parse("Tom Sorell, G. A. J. Rogers, and Jill Kaye, eds. <i>Scientia in Early Modern "
                                "Philosophy</i> . Dordrecht: Springer, 2010. Pp. 139.")
        self.assertEqual((t, l), ("Scientia in Early Modern Philosophy", "Kaye"))
        self.assertIn("G. A. J. Rogers", f)

    def test_review_essay_heading_glued_before_the_names(self):
        t, f, l, _ = self.parse("The Rise of the “Mechanical Philosophy”: Current Scholarship on "
                                "Seventeenth-Century ScienceDaniel Garber and Sophie Roux. <i>The Mechanization "
                                "of Natural Philosophy</i> . Dordrecht: Springer, 2013. Pp. xviii+327.")
        self.assertEqual((t, f, l), ("The Mechanization of Natural Philosophy", "Daniel Garber and Sophie", "Roux"))

    def test_a_surname_with_an_inner_capital_is_not_split(self):
        _, f, l, _ = self.parse("Snezana Lawrence and Mark McCartney, eds. <i>Mathematicians and Their Gods</i> . "
                                "Oxford: Oxford University Press, 2015. Pp. 320.")
        self.assertEqual((f, l), ("Snezana Lawrence and Mark", "McCartney"))

    def test_an_article_with_an_italic_phrase_is_not_a_citation(self):
        """No publisher, year or page count follows, so this rule must not fire."""
        from crossref_parsing import _parse_italic_citation
        self.assertIsNone(_parse_italic_citation("Cause and Effect in Leibniz’s <i>Brevis demonstratio</i>"))


class NoAuthorsFromTitleWords(unittest.TestCase):
    def test_phrases_are_recognised(self):
        for first, last in (("One", "Man"), ("Shattered", "Dreams"), ("Thought", "Experiments")):
            self.assertTrue(looks_like_a_phrase(first, last), f"{first} {last}")

    def test_real_names_are_not(self):
        for first, last in (("Nicholas", "Buccola"), ("Brandon M.", "Terry"), ("Quentin", "Skinner"),
                            ("Paul", "Wood"), ("Lloyd P.", "Gerson")):
            self.assertFalse(looks_like_a_phrase(first, last), f"{first} {last}")


class SuspectHighlighting(unittest.TestCase):
    def test_title_openings_in_the_author_field_are_flagged(self):
        for first, last in (("The", "Significance"), ("Kant and", "Philosophy"), ("The Cambridge", "Companion"),
                            ("Mathematicians and", "Their"), ("THANK YOU", "TO"), ("Historical", "Perspectives")):
            self.assertTrue(author_reads_as_a_title(first, last), f"{first} {last}")

    def test_names_and_name_lists_are_not_flagged(self):
        for first, last in (("Tad M.", "Schmaltz"), ("W. D.", "Hart"), ("Thomas Reid and Paul", "Wood"),
                            ("Matthew Benjamin", "Cole"), ("Aidan", "Nichols, O.P."),
                            ("Mélanie Frappier, Letitia Meynell and James R.", "Brown")):
            self.assertFalse(author_reads_as_a_title(first, last), f"{first} {last}")

    def test_a_title_made_only_of_people_is_flagged(self):
        self.assertTrue(title_is_only_names("Michael Heidelberger and Gregor Schiemann"))
        self.assertTrue(title_is_only_names("Uljana Feest"))
        for real in ("Stoic Eros", "Kant and Philosophy of Science Today", "Hegel's Ladder"):
            self.assertFalse(title_is_only_names(real), real)

    def test_the_review_page_uses_the_same_checks(self):
        import review_app
        r = review_app._suspect_reasons("The", "Significance", "Michael Heidelberger and Gregor Schiemann",
                                        "review", "David J.", "Stump")
        self.assertIn("author:reads-as-title", r)
        self.assertIn("title:only-names", r)
        self.assertEqual(review_app._suspect_reasons("Nicholas", "Buccola", "One Man's Freedom", "review",
                                                     "Dave", "Bridge"), [])


if __name__ == "__main__":
    unittest.main()
