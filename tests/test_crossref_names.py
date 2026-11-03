"""Journals whose Crossref deposit name differs from ours.

Crossref filters container-title by exact match, so the scan must search
under Crossref's name and still file reviews under ours.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import crossref_scraper as cs
from journals import JOURNALS


class CrossrefNames(unittest.TestCase):
    def setUp(self):
        self.s = cs.CrossrefReviewScraper()

    def test_config_carries_crossref_name(self):
        self.assertEqual(JOURNALS['The Philosophical Review']['crossref_name'], 'Philosophical Review')

    def test_review_filed_under_our_name(self):
        item = {
            'title': ['<i>The Philosophy of Envy</i>'],
            'DOI': '10.1215/00318108-0000000',
            'container-title': ['Philosophical Review'],
            'author': [{'given': 'Heidi', 'family': 'Maibom'}],
            'issued': {'date-parts': [[2025, 4]]},
        }
        r = self.s.extract_review(item)
        self.assertEqual(r['Publication Source'], 'The Philosophical Review')

    def test_escaped_ampersand_name_maps_back(self):
        self.assertEqual(self.s._our_name.get('Philosophy & Rhetoric'), 'Philosophy & Rhetoric')


if __name__ == '__main__':
    unittest.main()
