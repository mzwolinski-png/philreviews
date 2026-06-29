"""Unit tests for the Philosophia symposium detector (pure functions)."""
import os, sys, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import philosophia_symposia as ps


class Helpers(unittest.TestCase):
    def test_norm_unescapes_and_strips(self):
        self.assertEqual(ps._norm("Précis &amp; Co."), "precis co")

    def test_strip_book_noise(self):
        self.assertEqual(
            ps._strip_book_noise("Nenad Miscevic, Thought Experiments (Springer, 2022)",
                                 ["Nenad Miscevic"]),
            "Thought Experiments")

    def test_parse_reply_commentators(self):
        got = ps._parse_reply_commentators(
            "Replies to Fassio, Schleifer McCormick, Finlay, and Schmidt")
        self.assertEqual(got, ["fassio", "mccormick", "finlay", "schmidt"])

    def test_reviewer_fields_dual_author(self):
        self.assertEqual(ps._reviewer_fields(["Bob Fischer", "Meghan Barrett"]),
                         ("Bob Fischer and Meghan", "Barrett"))
        self.assertEqual(ps._reviewer_fields(["John Smith"]), ("John", "Smith"))


class DetectSymposia(unittest.TestCase):
    def test_disambiguates_shared_surname(self):
        # Reply names "Cohen"; two different Cohens are in the window. Only the
        # one whose commentary engages the book may be admitted.
        items = [
            {"doi": "p", "title": "Précis of Worldmaking",
             "authors": ["Jane Author"], "date": [2025, 1, 1], "page": None},
            {"doi": "a", "title": "Comments on Worldmaking",
             "authors": ["Alice Cohen"], "date": [2025, 2, 1], "page": None},
            {"doi": "b", "title": "A Separate Topic Entirely",
             "authors": ["Bob Cohen"], "date": [2025, 2, 1], "page": None},
            {"doi": "r", "title": "Replies to Cohen",
             "authors": ["Jane Author"], "date": [2025, 3, 1], "page": None},
        ]
        symp = ps.detect_symposia(items)
        self.assertEqual(len(symp), 1)
        dois = {pc["doi"] for pc in symp[0]["pieces"]}
        self.assertEqual(dois, {"p", "a", "r"})   # Bob Cohen ('b') excluded
        self.assertEqual(symp[0]["book"], "Worldmaking")


if __name__ == "__main__":
    unittest.main()
