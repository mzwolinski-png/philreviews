"""Marx & Philosophy Review of Books: everything comes off the listing page.

The journal has no DOIs, so until 2026-10-11 the index held one of its ~1,700
reviews. Fixtures are the live listing saved that day: page 1 (2026) and
page 100 (2016, where the date shares its span with a comments badge).
"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import marx_philosophy_scraper as mp  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "marx_philosophy")


def load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return mp.parse_listing(f.read())


class Listing(unittest.TestCase):
    def test_every_entry_on_the_page_is_read(self):
        self.assertEqual(len(load("listing_page1.html")), 10)
        self.assertEqual(len(load("listing_page100.html")), 10)

    def test_a_current_entry(self):
        r = load("listing_page1.html")[0]
        self.assertEqual(r["book_title"], "The Dialectics of Ecology")
        self.assertEqual((r["book_author_first_name"], r["book_author_last_name"]), ("John Bellamy", "Foster"))
        self.assertEqual((r["reviewer_first_name"], r["reviewer_last_name"]), ("Nicholas", "Denning"))
        self.assertEqual(r["publication_date"], "2026-10-08")
        self.assertTrue(r["review_link"].startswith("https://marxandphilosophy.org.uk/reviews/22942_"))
        self.assertEqual(r["access_type"], "Open")

    def test_two_authors_follow_the_project_convention(self):
        r = load("listing_page1.html")[2]
        self.assertEqual((r["book_author_first_name"], r["book_author_last_name"]),
                         ("Isabel Jacobs and Jacob", "Saliba"))

    def test_date_is_read_past_the_comments_badge(self):
        r = load("listing_page100.html")[0]
        self.assertEqual(r["publication_date"], "2016-06-06")
        self.assertEqual(r["book_author_last_name"], "North")

    def test_every_entry_has_title_link_and_date(self):
        for name in ("listing_page1.html", "listing_page100.html"):
            for r in load(name):
                self.assertTrue(r["book_title"] and r["review_link"] and r["publication_date"], r)


class Names(unittest.TestCase):
    def test_particles_and_suffixes_stay_with_the_surname(self):
        self.assertEqual(mp.split_person("Hent de Vries"), ("Hent", "de Vries"))
        self.assertEqual(mp.split_person("James W. Ely, Jr."), ("James W.", "Ely Jr."))
        self.assertEqual(mp.split_person("Plato"), ("", "Plato"))

    def test_three_authors(self):
        self.assertEqual(mp.author_fields(["A. Greif", "J. Mokyr", "Guido Tabellini"]),
                         ("A. Greif, J. Mokyr and Guido", "Tabellini"))


if __name__ == "__main__":
    unittest.main()
