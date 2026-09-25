"""An arriving DOI attaches to the review we already hold without one.

TOC-alert reviews are often entered by hand before the publisher deposits with
Crossref, and the Philosopher's Index carries no DOIs. Insert dedupe was by DOI
and link only, so the Crossref record for the same review later went in as a
second row. Measured 2026-09-25: about 500 such pairs across the database.
"""
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import db as real_db  # noqa: E402

SEARCH = "https://www.google.com/search?q=%22Democracy+Despite+Itself%22"


def rec(**kw):
    base = {"publication_source": "Political Theory",
            "book_title": "Democracy Despite Itself: Liberal Constitutionalism and Militant Democracy",
            "book_author_first_name": "Benjamin A.", "book_author_last_name": "Schupmann",
            "reviewer_first_name": "Eraldo", "reviewer_last_name": "Souza dos Santos",
            "publication_date": "2026-09-01", "access_type": "Restricted",
            "entry_type": "review"}
    base.update(kw)
    return base


class AttachDoi(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self._orig = real_db.DB_PATH
        real_db.DB_PATH = os.path.join(self.tmp, "t.db")
        real_db.init_db()
        # the hand-entered row: no DOI, a search link standing in
        real_db.insert_review(rec(review_link=SEARCH, doi=None))

    def tearDown(self):
        real_db.DB_PATH = self._orig
        shutil.rmtree(self.tmp)

    def rows(self):
        c = sqlite3.connect(real_db.DB_PATH)
        return c.execute("SELECT doi, review_link FROM reviews").fetchall()

    def test_crossref_record_attaches_instead_of_duplicating(self):
        real_db.insert_reviews([rec(doi="10.1177/x1", review_link="https://doi.org/10.1177/x1")])
        self.assertEqual(self.rows(), [("10.1177/x1", "https://doi.org/10.1177/x1")])

    def test_single_insert_path_attaches_too(self):
        """The weekly delta uses insert_review, the full scrape insert_reviews."""
        real_db.insert_review(rec(doi="10.1177/x1", review_link="https://doi.org/10.1177/x1"))
        self.assertEqual(len(self.rows()), 1)

    def test_case_and_curly_apostrophes_do_not_defeat_the_match(self):
        """The PI rows that doubled up differed from Crossref by exactly this."""
        real_db.insert_review(rec(book_title="Marx's Ethical Vision", reviewer_last_name="Keller",
                                  review_link="https://ebsco.com/x", doi=None))
        real_db.insert_reviews([rec(book_title="MARX’S ETHICAL VISION", reviewer_last_name="Keller",
                                    doi="10.1515/x5", review_link="https://doi.org/10.1515/x5")])
        self.assertEqual(len(self.rows()), 2)  # the Schupmann row plus one Marx row
        c = sqlite3.connect(real_db.DB_PATH)
        self.assertEqual(c.execute("SELECT doi FROM reviews WHERE reviewer_last_name='Keller'")
                         .fetchall(), [("10.1515/x5",)])

    def test_a_different_book_with_the_same_prefix_is_not_merged(self):
        """One reviewer can review several 'Rudolf Carnap: ...' volumes for one journal."""
        real_db.insert_reviews([rec(book_title="Democracy Despite Itself: A Different Subtitle",
                                    doi="10.1177/x2", review_link="https://doi.org/10.1177/x2")])
        self.assertEqual(len(self.rows()), 2)

    def test_a_different_reviewer_is_a_different_review(self):
        real_db.insert_reviews([rec(reviewer_last_name="Kirshner", doi="10.1177/x3",
                                    review_link="https://doi.org/10.1177/x3")])
        self.assertEqual(len(self.rows()), 2)

    def test_symposium_pieces_are_never_merged(self):
        real_db.insert_reviews([rec(entry_type="symposium", doi="10.1177/x4",
                                    review_link="https://doi.org/10.1177/x4")])
        self.assertEqual(len(self.rows()), 2)

    def test_a_real_existing_link_is_kept(self):
        c = sqlite3.connect(real_db.DB_PATH)
        c.execute("UPDATE reviews SET review_link='https://journal.example/review/9'")
        c.commit()
        real_db.insert_reviews([rec(doi="10.1177/x1", review_link="https://doi.org/10.1177/x1")])
        self.assertEqual(self.rows(), [("10.1177/x1", "https://journal.example/review/9")])



class DoiCase(unittest.TestCase):
    """Crossref returns 10.1017/s0034..., a publisher's TOC shows 10.1017/S0034..."""
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self._orig = real_db.DB_PATH
        real_db.DB_PATH = os.path.join(self.tmp, "t.db")
        real_db.init_db()

    def tearDown(self):
        real_db.DB_PATH = self._orig
        shutil.rmtree(self.tmp)

    def test_upper_and_lower_case_doi_are_one_review(self):
        real_db.insert_review(rec(doi="10.1017/S0034412526101917",
                                  review_link="https://doi.org/10.1017/S0034412526101917"))
        real_db.insert_reviews([rec(doi="10.1017/s0034412526101917",
                                    review_link="https://doi.org/10.1017/s0034412526101917")])
        c = sqlite3.connect(real_db.DB_PATH)
        self.assertEqual(c.execute("SELECT doi FROM reviews").fetchall(),
                         [("10.1017/s0034412526101917",)])

    def test_doi_exists_ignores_case(self):
        real_db.insert_review(rec(doi="10.1017/s0034412526101917", review_link="x"))
        self.assertTrue(real_db.doi_exists("10.1017/S0034412526101917"))


if __name__ == "__main__":
    unittest.main()
