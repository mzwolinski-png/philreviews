"""Regression tests for the extracted Crossref title/author parser."""
import os, sys, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import crossref_parsing as cp


class IsBookReview(unittest.TestCase):
    def test_page_count_heuristic(self):
        self.assertTrue(cp.is_book_review(
            {"title": ["Justice. 320 pp"], "type": "journal-article"}, "all"))

    def test_italic_only_skips_plain_article(self):
        self.assertFalse(cp.is_book_review(
            {"title": ["Plain Article"], "type": "journal-article"}, "italic_only"))


class ParseReviewTitle(unittest.TestCase):
    def test_italic_title_only(self):
        r = cp.parse_review_title("<i>A Theory of Justice</i>")
        self.assertEqual(r["book_title"], "A Theory of Justice")
        self.assertEqual(r["format"], "italic_title_only")
        self.assertTrue(r["needs_doi_scrape"])  # author looked up later

    def test_title_by_author(self):
        r = cp.parse_review_title("The Republic by Plato (review)")
        self.assertEqual(r["book_title"], "The Republic")
        self.assertEqual(r["book_author_last"], "Plato")
        self.assertEqual(r["format"], "title_by_author")

    def test_italic_tags_with_author(self):
        r = cp.parse_review_title("Smith, John. <i>Ethics</i>. OUP, 2020")
        self.assertEqual(r["book_title"], "Ethics")
        self.assertEqual(r["book_author_first"], "John")
        self.assertEqual(r["book_author_last"], "Smith")
        self.assertEqual(r["format"], "italic_tags")


if __name__ == "__main__":
    unittest.main()
