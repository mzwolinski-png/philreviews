"""A Crossref title that is only an italicized book title is still a review.

Critical Inquiry deposits ":<i>Title</i>" since 2025, with the book author's
name dropped before the colon. The parser declines it, so extract_review must
fall back to the italic text and leave the author for enrichment.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import crossref_scraper as cs


def _item(title):
    return {
        'title': [title],
        'DOI': '10.1086/738431',
        'container-title': ['Critical Inquiry'],
        'author': [{'given': 'Jan', 'family': 'Ruda'}],
        'issued': {'date-parts': [[2026, 1]]},
        'page': '395-397',
    }


class BareItalicTitle(unittest.TestCase):
    def setUp(self):
        self.s = cs.CrossrefReviewScraper()

    def test_leading_colon_and_italic(self):
        r = self.s.extract_review(_item(":\n   <i>How to Research like a Dog: Kafka’s New Science</i>"))
        self.assertIsNotNone(r)
        self.assertEqual(r['Book Title'], "How to Research like a Dog: Kafka's New Science")
        self.assertEqual(r['Book Author Last Name'], '')

    def test_italic_only_without_colon(self):
        r = self.s.extract_review(_item("<i>The Soviet Century: Archaeology of a Lost World</i>"))
        self.assertEqual(r['Book Title'], "The Soviet Century: Archaeology of a Lost World")

    def test_two_italic_runs_are_not_taken_whole(self):
        # an essay title quoting two books is not a bare book title
        r = self.s.extract_review(_item("<i>Hegel</i> and <i>Marx</i>"))
        if r:
            self.assertNotEqual(r['Book Title'], "Hegel</i> and <i>Marx")


if __name__ == '__main__':
    unittest.main()
