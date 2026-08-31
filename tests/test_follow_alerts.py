"""Tests for the follow/alert matching logic."""
import os, sys, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import db
from send_follow_alerts import match_follows, _title_matches, _author_matches


def _f(ftype, value, email="a@b.edu", token="t1"):
    return {"email": email, "follow_type": ftype, "follow_value": value,
            "norm_value": db.norm_follow_value(value), "token": token}


def _r(title, af="", al="", journal="J"):
    return {"book_title": title, "book_author_first_name": af,
            "book_author_last_name": al, "reviewer_first_name": "R",
            "reviewer_last_name": "Viewer", "publication_source": journal,
            "review_link": "https://x", "doi": ""}


class TitleMatching(unittest.TestCase):
    def test_exact_and_diacritics(self):
        self.assertTrue(_title_matches(db.norm_follow_value("Playing Possum"),
                                       db.norm_follow_value("Playing Possum")))

    def test_subtitle_variant(self):
        short = db.norm_follow_value("In Covid's Wake")
        full = db.norm_follow_value("In Covid's Wake: How Our Politics Failed Us")
        self.assertTrue(_title_matches(short, full))
        self.assertTrue(_title_matches(full, short))

    def test_short_prefix_does_not_overmatch(self):
        # "Justice" must not match every title starting with the word
        self.assertFalse(_title_matches(db.norm_follow_value("Justice"),
                                        db.norm_follow_value("Justice for Animals: Our Collective Responsibility")))


class AuthorMatching(unittest.TestCase):
    def test_simple(self):
        self.assertTrue(_author_matches(db.norm_follow_value("Susana Monsó"),
                                        db.norm_follow_value("Susana Monso")))

    def test_joined_multi_author(self):
        joined = db.norm_follow_value("Stephen Macedo and Frances Lee")
        self.assertTrue(_author_matches(db.norm_follow_value("Frances Lee"), joined))
        self.assertTrue(_author_matches(db.norm_follow_value("Stephen Macedo"), joined))

    def test_no_partial_name_match(self):
        # follow "Dan Lee" must not match "Daniel Lee" or "Frances Lee"
        self.assertFalse(_author_matches(db.norm_follow_value("Dan Lee"),
                                         db.norm_follow_value("Daniel Lee")))


class EndToEnd(unittest.TestCase):
    def test_grouping_per_email(self):
        follows = [_f("book", "In Covid's Wake", token="t1"),
                   _f("author", "Frances Lee", token="t2"),
                   _f("book", "Unrelated Book", email="c@d.edu", token="t3")]
        reviews = [_r("In Covid's Wake: How Our Politics Failed Us",
                      "Stephen Macedo and Frances", "Lee")]
        out = match_follows(follows, reviews)
        self.assertEqual(set(out.keys()), {"a@b.edu"})
        self.assertEqual(len(out["a@b.edu"]), 2)  # book hit + author hit
        self.assertEqual(out["a@b.edu"][0]["reviews"][0]["book_author"],
                         "Stephen Macedo and Frances Lee")


if __name__ == "__main__":
    unittest.main()
