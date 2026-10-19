"""Title matching in the mainstream scraper.

2026-10-18: 23 false matches in one run. Function words counted as title
evidence, so a Guardian review of Bryan Washington's novel passed as a review
of "Likeness and Likelihood in the Presocratics and Plato" by Bryan.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import mainstream_review_scraper as m

G = "https://www.theguardian.com/books/"


class TitleMatching(unittest.TestCase):
    def test_function_words_are_not_significant(self):
        self.assertEqual(
            m.get_significant_words("Likeness and Likelihood in the Presocratics and Plato"),
            ["Likeness", "Likelihood", "Presocratics", "Plato"])

    def test_unrelated_review_sharing_a_surname_is_rejected(self):
        t = "Family Meal by Bryan Washington review – sexploits and ennui"
        trail = "The author of the acclaimed Memorial returns with a story of grief and the search for love"
        self.assertFalse(m.verify_result(
            t, f"{t} {trail}", G + "2023/nov/01/family-meal-by-bryan-washington-review",
            "Likeness and Likelihood in the Presocratics and Plato", "Bryan"))

    def test_roundup_with_a_matching_surname_is_rejected(self):
        t = "The best recent poetry – review roundup"
        trail = "Clarke on the light and the dark, and a new collection from the one poet"
        self.assertFalse(m.verify_result(
            t, f"{t} {trail}", G + "2026/oct/02/the-best-recent-poetry-review-roundup",
            "Aristotle and the Eleatic One", "Clarke"))

    def test_real_review_still_matches(self):
        t = "Why Socrates Died – review"
        self.assertTrue(m.verify_result(
            t, f"{t} Robin Waterfield's Why Socrates Died: Dispelling the Myths revisits the trial",
            G + "2010/feb/14/why-socrates-died-book-review",
            "Why Socrates Died: Dispelling the Myths", "Waterfield"))


if __name__ == "__main__":
    unittest.main()
