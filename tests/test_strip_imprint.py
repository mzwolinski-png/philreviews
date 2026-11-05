"""BJHS deposits the full citation with no separator between title and place."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import crossref_parsing as cp
import crossref_scraper as cs


class StripImprint(unittest.TestCase):
    def test_place_glued_to_title(self):
        self.assertEqual(cp.strip_trailing_imprint(
            'Proxies: The Cultural Work of Standing In London: MIT Press, 2021. Pp. 228'),
            'Proxies: The Cultural Work of Standing In')

    def test_two_word_city_and_state(self):
        self.assertEqual(cp.strip_trailing_imprint(
            'The Poetics of Natural History, 2nd edn New Brunswick, NJ: Rutgers University Press, 2019. Pp. 403'),
            'The Poetics of Natural History, 2nd edn')

    def test_city_list(self):
        self.assertEqual(cp.strip_trailing_imprint(
            'Going Underground: The Science and History of Falling through the Earth. New Jersey, '
            'London, Singapore, Beijing, Shanghai, Hong Kong, Taipei, Chennai and Tokyo: World '
            'Scientific, 2019. Pp. xi + 276'),
            'Going Underground: The Science and History of Falling through the Earth')

    def test_city_in_real_title_kept(self):
        for t in ('Oxford: A History of the City, 1100-1800', 'The Cambridge Companion to Kant',
                  'London: A Biography'):
            self.assertEqual(cp.strip_trailing_imprint(t), t)


class Corrigendum(unittest.TestCase):
    def test_corrigendum_is_not_a_review(self):
        item = {'title': ['Dylan Mulvin, <i>Proxies: The Cultural Work of Standing In</i> London: '
                          'MIT Press, 2021. Pp. 228. – CORRIGENDUM'],
                'DOI': '10.1017/s0007087423000249',
                'container-title': ['The British Journal for the History of Science'],
                'author': [{'given': 'John', 'family': 'Law'}],
                'issued': {'date-parts': [[2023]]}}
        self.assertIsNone(cs.CrossrefReviewScraper().extract_review(item))


if __name__ == '__main__':
    unittest.main()
