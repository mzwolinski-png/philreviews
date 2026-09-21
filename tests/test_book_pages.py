"""Book pages, server-rendered search, and the sitemap index.

These exist because 227k reviews were unreachable by search engines: the only
indexable URLs were the homepage, 1,503 journal pages and 15 subfields.
The book page — not a per-review page — is the unit, since we hold no review
text and 227k one-line pages would read as thin content.
"""
import json
import os
import re
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import app as appmod
import build_books


class BookKeyIdentity(unittest.TestCase):
    """Identity must never merge two different books."""

    def test_same_book_same_key(self):
        self.assertEqual(
            build_books.book_key("The Right to Sex", "Srinivasan"),
            build_books.book_key("the  right to   sex!", "srinivasan"))

    def test_accents_and_punctuation_fold(self):
        self.assertEqual(build_books.book_key("Être et Néant", "Sartre"),
                         build_books.book_key("Etre et Neant", "Sartre"))

    def test_different_authors_never_merge(self):
        self.assertNotEqual(build_books.book_key("Ethics", "Aristotle"),
                            build_books.book_key("Ethics", "Spinoza"))

    def test_subtitles_are_not_dropped(self):
        # dropping them would merge Volume 1 with Volume 2
        self.assertNotEqual(
            build_books.book_key("Collected Papers: Volume 1", "Quine"),
            build_books.book_key("Collected Papers: Volume 2", "Quine"))

    def test_empty_title_has_no_key(self):
        self.assertIsNone(build_books.book_key("", "Rawls"))


class SlugGeneration(unittest.TestCase):
    def test_slug_is_url_safe_and_readable(self):
        slug = build_books.make_slug("The Right to Sex: Feminism!", "Srinivasan", set())
        self.assertRegex(slug, r"^[a-z0-9-]+$")
        self.assertIn("right-to-sex", slug)
        self.assertTrue(slug.endswith("srinivasan"))

    def test_collisions_get_a_suffix(self):
        taken = set()
        a = build_books.make_slug("Ethics", "Smith", taken)
        b = build_books.make_slug("Ethics", "Smith", taken)
        self.assertNotEqual(a, b)

    def test_untitled_never_produces_an_empty_slug(self):
        self.assertTrue(build_books.make_slug("!!!", "", set()))


class BookPage(unittest.TestCase):
    def setUp(self):
        appmod.app.config["TESTING"] = True
        self.c = appmod.app.test_client()
        appmod._rate_limiter._hits.clear()

    def _a_multi_review_book(self):
        import db
        rows = db.get_books_for_sitemap(min_reviews=3, limit=1)
        return rows[0]["slug"] if rows else None

    def test_renders_and_lists_every_review(self):
        import db
        slug = self._a_multi_review_book()
        self.assertIsNotNone(slug, "no multi-review book in the database")
        data = db.get_book(slug)
        html = self.c.get(f"/book/{slug}").get_data(as_text=True)
        self.assertEqual(len(data["reviews"]), data["book"]["review_count"])
        self.assertEqual(html.count("<tr>") - 1, data["book"]["review_count"])

    def test_unknown_slug_is_404(self):
        self.assertEqual(self.c.get("/book/no-such-book-at-all").status_code, 404)

    def test_structured_data_is_a_book_with_reviews(self):
        slug = self._a_multi_review_book()
        html = self.c.get(f"/book/{slug}").get_data(as_text=True)
        blob = re.search(r'application/ld\+json">(.*?)</script>', html, re.S).group(1)
        d = json.loads(blob)                      # must be valid JSON
        self.assertEqual(d["@type"], "Book")
        self.assertTrue(d["url"].startswith("https://philreviews.org/book/"))
        self.assertTrue(all(r["@type"] == "Review" for r in d["review"]))

    def test_page_is_canonical_and_self_referencing(self):
        slug = self._a_multi_review_book()
        html = self.c.get(f"/book/{slug}").get_data(as_text=True)
        self.assertIn(f'rel="canonical" href="https://philreviews.org/book/{slug}"', html)


class SearchPage(unittest.TestCase):
    def setUp(self):
        appmod.app.config["TESTING"] = True
        self.c = appmod.app.test_client()
        appmod._rate_limiter._hits.clear()

    def test_query_renders_results_server_side(self):
        html = self.c.get("/search?q=rawls").get_data(as_text=True)
        self.assertIn("<table", html)
        self.assertIn("rawls", html.lower())

    def test_results_link_to_book_pages(self):
        html = self.c.get("/search?q=rawls").get_data(as_text=True)
        self.assertGreater(len(re.findall(r'href="/book/', html)), 0)

    def test_empty_query_redirects_home(self):
        self.assertEqual(self.c.get("/search").status_code, 302)

    def test_no_results_is_a_200_not_an_error(self):
        r = self.c.get("/search?q=zzzzznotathing")
        self.assertEqual(r.status_code, 200)
        self.assertIn("No reviews match", r.get_data(as_text=True))

    def test_canonical_includes_the_query(self):
        html = self.c.get("/search?q=kant").get_data(as_text=True)
        self.assertIn('rel="canonical" href="https://philreviews.org/search?q=kant"', html)


class Sitemap(unittest.TestCase):
    def setUp(self):
        appmod.app.config["TESTING"] = True
        self.c = appmod.app.test_client()

    def test_root_is_an_index_of_sitemaps(self):
        xml = self.c.get("/sitemap.xml").get_data(as_text=True)
        self.assertIn("<sitemapindex", xml)
        self.assertIn("/sitemap-pages.xml", xml)
        self.assertIn("/sitemap-books-1.xml", xml)

    def test_book_sitemap_respects_the_50k_limit(self):
        xml = self.c.get("/sitemap-books-1.xml").get_data(as_text=True)
        self.assertLessEqual(xml.count("<url>"), 50000)
        self.assertIn("/book/", xml)

    def test_pages_sitemap_still_lists_journals(self):
        xml = self.c.get("/sitemap-pages.xml").get_data(as_text=True)
        self.assertIn("/journal/", xml)
        self.assertIn("/subfield/", xml)

    def test_part_past_the_end_is_404(self):
        self.assertEqual(self.c.get("/sitemap-books-9999.xml").status_code, 404)

    def test_search_action_points_at_a_crawlable_url(self):
        html = self.c.get("/").get_data(as_text=True)
        self.assertIn("philreviews.org/search?q={search_term_string}", html)
        self.assertNotIn("#q={search_term_string}", html)
