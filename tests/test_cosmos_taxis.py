"""Cosmos + Taxis issue parsing.

C+T deposits nothing with Crossref, so these issue pages are the only route to
the journal and there is no second source to catch a parser regression. The
fixtures are five real pages chosen for the things that broke the first
scraper: the two name-case eras, a symposium whose reply has a real title, a
contribution called "Symposium Prologue" that looks like a section heading,
and an "In Memoriam: <name>" heading whose colon hid it from the skip list.
"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from cosmos_taxis_scraper import (  # noqa: E402
    CosmosTaxisScraper, _author_fields, _book_author_from_title,
    _proper_name, _split_name,
)

FIX = os.path.join(ROOT, "tests", "fixtures")


def parse(slug, year):
    path = os.path.join(FIX, f"cosmos_taxis_{slug}.html")
    with open(path, encoding="utf-8") as fh:
        html = fh.read()
    return CosmosTaxisScraper().parse_issue(f"https://cosmosandtaxis.org/{slug}/",
                                            year, html=html)


class Names(unittest.TestCase):
    def test_caps_names_are_folded_back(self):
        self.assertEqual(_proper_name("CHRISTOPHER ADAIR-TOTEFF"), "Christopher Adair-Toteff")
        self.assertEqual(_proper_name("THOMAS J. MCQUADE"), "Thomas J. McQuade")
        self.assertEqual(_proper_name("O'BRIEN"), "O'Brien")

    def test_particles_stay_lowercase(self):
        self.assertEqual(_proper_name("EDWIN VAN DE HAAR"), "Edwin van de Haar")

    def test_connectors_do_not_defeat_caps_detection(self):
        # not str.isupper() because of "and", but still a caps name
        self.assertEqual(_proper_name("GEORGE STEIRIS and GEORGE POLITIS"),
                         "George Steiris and George Politis")

    def test_mixed_case_names_pass_through_untouched(self):
        """Pre-2025 issues print names correctly; re-capitalising them is a bug."""
        for name in ("Gus diZerega", "Edwin van de Haar", "Jean-Luc Marion"):
            self.assertEqual(_proper_name(name), name)

    def test_particle_travels_with_the_surname(self):
        self.assertEqual(_split_name("Edwin van de Haar"), ("Edwin", "van de Haar"))
        self.assertEqual(_split_name("Scott Scheall"), ("Scott", "Scheall"))


class TitleParsing(unittest.TestCase):
    def test_by_author_is_split_off_the_title(self):
        t, a = _book_author_from_title("Finding the Mother Tree by Suzanne Simard")
        self.assertEqual(t, "Finding the Mother Tree")
        self.assertEqual(a, "Suzanne Simard")

    def test_eds_form_is_handled(self):
        t, a = _book_author_from_title(
            "Critics of Enlightenment Rationalism, eds. Gene Callahan and Kenneth McIntyre")
        self.assertEqual(t, "Critics of Enlightenment Rationalism")
        self.assertEqual(a, "Gene Callahan and Kenneth McIntyre")

    def test_by_inside_a_real_title_is_left_alone(self):
        """'by' is common in titles; only a short name-like tail counts."""
        title = ("Governed by the Market? A Study of How Institutions Shape the Long Run "
                 "Behaviour of Prices and Wages Across Two Centuries of European History")
        t, a = _book_author_from_title(title)
        self.assertEqual(a, "")
        self.assertEqual(t, title)

    def test_multi_author_convention(self):
        first, last = _author_fields("Jobst Landgrebe and Barry Smith")
        self.assertEqual(last, "Smith")
        self.assertIn("Landgrebe", first)


class IssueParsing(unittest.TestCase):
    def test_caps_era_issue(self):
        recs = parse("ct_1356", 2025)
        titles = [r["book_title"] for r in recs]
        self.assertIn("Imagining After Capitalism", titles)
        # the In Memoriam notice for Paul Lewis must not arrive as a book
        self.assertNotIn("Paul Lewis", titles)
        self.assertTrue(all("Authors Index" not in t for t in titles))

    def test_title_case_era_keeps_the_book_author(self):
        recs = parse("ct_1256", 2024)
        ikeda = [r for r in recs if r["book_title"].startswith("A City Cannot Be a Work of Art")]
        self.assertEqual(len(ikeda), 1)
        self.assertEqual(ikeda[0]["book_author_last_name"], "Ikeda")
        self.assertEqual(ikeda[0]["reviewer_last_name"], "Andersson")

    def test_every_record_carries_its_own_pdf_link(self):
        for slug, year in (("ct_1356", 2025), ("ct_1256", 2024), ("ct_1412", 2026)):
            for r in parse(slug, year):
                self.assertTrue(r["review_link"].lower().endswith(".pdf"), r["review_link"])

    def test_articles_and_in_memoriam_are_excluded(self):
        """Only the review-ish sections count, or the index fills with articles."""
        recs = parse("ct_1412", 2026)
        titles = [r["book_title"] for r in recs]
        self.assertIn("Liberty as Independence", titles)                  # critical notice
        self.assertNotIn("The 15-Minute City is a Tree", titles)          # article
        self.assertNotIn("Frederick Turner: Poet of Autopoiesis", titles)  # in memoriam

    def test_in_memoriam_heading_with_a_colon_still_skips(self):
        """'In Memoriam: David F. Hardwick' once let a person through as a book."""
        titles = [r["book_title"] for r in parse("ct_91112", 2021)]
        self.assertNotIn("Gus diZerega", titles)
        self.assertIn("Finding the Mother Tree", titles)


class Symposia(unittest.TestCase):
    def test_contributions_share_one_group_and_the_book_title(self):
        recs = [r for r in parse("ct_1256", 2024) if r["entry_type"] == "symposium"]
        self.assertGreater(len(recs), 5)
        self.assertEqual(len({r["symposium_group"] for r in recs}), 1)
        self.assertTrue(all(r["book_title"].startswith("Why Machines Will Never Rule the World")
                            for r in recs))
        self.assertTrue(all(r["book_author_last_name"] == "Smith" for r in recs))

    def test_reply_is_found_by_author_even_with_a_real_title(self):
        """The reply here is titled 'Intelligence. And what computers still can't do'."""
        recs = parse("ct_1256", 2024)
        replies = [r for r in recs if "[Author's Reply]" in r["book_title"]]
        self.assertEqual(len(replies), 1)
        self.assertEqual(replies[0]["reviewer_last_name"], "Smith")

    def test_symposium_prologue_is_a_contribution_not_a_heading(self):
        """'Symposium Prologue' once reset the group's book title to 'Prologue'."""
        recs = [r for r in parse("ct_934", 2021) if r["entry_type"] == "symposium"]
        self.assertTrue(recs)
        for r in recs:
            self.assertTrue(r["book_title"].startswith(
                "F. A. Hayek and the Epistemology of Politics"), r["book_title"])
        labels = {r["book_title"].split("[")[1].rstrip("]")
                  for r in recs if "[" in r["book_title"]}
        self.assertEqual(labels, {"Symposium Introduction", "Précis", "Author's Reply"})


if __name__ == "__main__":
    unittest.main()
