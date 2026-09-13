"""The mainstream scraper must not accept front pages or index URLs.

Search engines return an outlet's home page, issue index or tag page when the
article they quote is paywalled. The snippet still carries the book's title and
author, so every content check passes and the row looks plausible — this is how
one Steven Lukes book acquired five unrelated NYRB URLs (2026-09-13).
"""
import os, sys, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import mainstream_review_scraper as ms


class ArticleUrlGate(unittest.TestCase):
    def test_rejects_front_page_and_indexes(self):
        for url in ["https://nybooks.com",
                    "https://nybooks.com/",
                    "https://nybooks.com/issues/2026/09/24",
                    "https://theguardian.com/books",
                    "https://nytimes.com/section/books",
                    "https://lareviewofbooks.org/tag/philosophy",
                    "https://lrb.co.uk/search?q=spinoza"]:
            self.assertFalse(ms.is_article_url(url), url)

    def test_accepts_real_articles(self):
        for url in ["https://nybooks.com/articles/2026/09/24/is-morality-universal-"
                    "the-diversity-of-morals-lukes",
                    "https://nybooks.com/online/2019/08/26/working-off-the-past",
                    "https://theguardian.com/books/2003/oct/04/featuresreviews.guardianreview",
                    "https://lrb.co.uk/the-paper/v43/n03/freya-johnston/a-slug",
                    "https://kirkusreviews.com/book-reviews/alister-mcgrath/"
                    "the-twilight-of-atheism"]:
            self.assertTrue(ms.is_article_url(url), url)

    def test_verify_result_rejects_index_url(self):
        # content checks would otherwise pass: author and title both present
        self.assertFalse(ms.verify_result(
            "The New York Review of Books",
            "Is Morality Universal? The Diversity of Morals by Steven Lukes, review",
            "https://nybooks.com/issues/2026/09/24",
            "The Diversity of Morals", "Lukes"))

    def test_verify_result_still_accepts_the_real_article(self):
        self.assertTrue(ms.verify_result(
            "Is Morality Universal?",
            "The Diversity of Morals by Steven Lukes, reviewed by Susan Neiman",
            "https://nybooks.com/articles/2026/09/24/is-morality-universal-"
            "the-diversity-of-morals-lukes",
            "The Diversity of Morals", "Lukes"))


class ReviewerExtraction(unittest.TestCase):
    """The reviewer is not the first name after "by" — that is the book's author."""

    def test_prefers_explicit_reviewer_phrasing(self):
        self.assertEqual(
            ms.extract_reviewer_from_snippet(
                "The Diversity of Morals by Steven Lukes, reviewed by Susan Neiman",
                "Lukes"),
            ("Susan", "Neiman"))

    def test_skips_the_book_author_when_no_explicit_phrasing(self):
        self.assertEqual(
            ms.extract_reviewer_from_snippet(
                "The Diversity of Morals by Steven Lukes. A searching study by Susan Neiman",
                "Lukes"),
            ("Susan", "Neiman"))

    def test_authors_own_article_still_returns_the_author(self):
        # verify_result relies on this to reject the author writing about their book
        self.assertEqual(
            ms.extract_reviewer_from_snippet("Camus and the crisis, by Robert Zaretsky",
                                             "Zaretsky"),
            ("Robert", "Zaretsky"))

    def test_plain_byline(self):
        self.assertEqual(
            ms.extract_reviewer_from_snippet("By Mark Lilla. The closing of the "
                                             "Straussian mind", "Lord"),
            ("Mark", "Lilla"))


class DuplicateDetection(unittest.TestCase):
    """Stored links keep the form they arrived in, so an exact string match
    misses "www." and per-book "#anchor" variants of the same article."""

    def test_normalise_collapses_www_and_fragment(self):
        self.assertEqual(
            ms.normalize_url("https://www.nybooks.com/articles/2024/06/20/the-tower/#a-book"),
            ms.normalize_url("https://nybooks.com/articles/2024/06/20/the-tower"))

    def test_already_indexed_matches_anchored_sibling(self):
        # the Mark Lilla review is stored with a per-book anchor and "www."
        url = ("https://nybooks.com/articles/2024/06/20/"
               "the-tower-and-the-sewer-why-liberalism-failed-deneen")
        self.assertTrue(ms.already_indexed(url, "Common Good Constitutionalism", "Vermeule"))

    def test_already_indexed_allows_a_different_book_at_one_url(self):
        # multi-book reviews legitimately share a URL
        url = ("https://nybooks.com/articles/2024/06/20/"
               "the-tower-and-the-sewer-why-liberalism-failed-deneen")
        self.assertFalse(ms.already_indexed(url, "A Book Nobody Wrote", "Nobody"))
