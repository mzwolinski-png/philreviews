import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import bmcr_scraper as bmcr

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'bmcr')


def load(name):
    with open(os.path.join(FIXTURES, name), encoding='utf-8') as fh:
        return fh.read()


class TestListing(unittest.TestCase):
    def setUp(self):
        self.new = {r['review_link']: r for r in bmcr.parse_listing(load('listing_2026.html'))}
        self.old = {r['review_link']: r for r in bmcr.parse_listing(load('listing_1995.html'))}

    def test_modern_entry(self):
        r = self.new['https://bmcr.brynmawr.edu/2026/2026.01.05/']
        self.assertEqual(r['book_title'], 'Sextus Empiricus Against the Arithmeticians')
        self.assertEqual((r['book_author_first_name'], r['book_author_last_name']), ('Lorenzo', 'Corti'))
        self.assertEqual((r['reviewer_first_name'], r['reviewer_last_name']), ('Sebastiano', 'Molinelli'))
        self.assertEqual(r['publication_date'], '2026-01-01')

    def test_books_received_is_skipped(self):
        self.assertNotIn('https://bmcr.brynmawr.edu/2026/2026.01.01/', self.new)

    def test_old_title_prefix_is_dropped(self):
        self.assertEqual(self.old['https://bmcr.brynmawr.edu/1995/1995.01.03/']['book_title'], 'Mission and Conversion')

    def test_ancient_author_keeps_the_book(self):
        r = self.old['https://bmcr.brynmawr.edu/1995/1995.01.06/']
        self.assertEqual(r['book_title'], 'The Epicurus Reader')
        self.assertEqual((r['book_author_first_name'], r['book_author_last_name']), ('', 'Epicurus'))


class TestCleaning(unittest.TestCase):
    def test_expanded_initials(self):
        self.assertEqual(bmcr.clean_authors('Louis Robert,  G. W. (Glen Warren) Bowersock'),
                         ['Louis Robert', 'G. W. Bowersock'])

    def test_catalogue_style_ancient_author(self):
        self.assertEqual(bmcr.clean_authors('of Cilicia Simplicius, J. O. Urmson, Peter Lautner'), ['Simplicius of Cilicia'])
        self.assertEqual(bmcr.clean_authors('Pierre Hadot, Emperor of Rome Marcus Aurelius'), ['Pierre Hadot'])
        self.assertEqual(bmcr.clean_authors('Maria Michela Sassi, Scuola normale superiore'), ['Maria Michela Sassi'])

    def test_editor_prefixes(self):
        self.assertEqual(bmcr.clean_title('ed., McKirahan, trans., Presocratics Reader', ['Patricia Curd', 'Richard D. McKirahan']),
                         'Presocratics Reader')
        self.assertEqual(bmcr.clean_title('ALSO SEEN: Ross, Aristotle, 6th ed', ['W. D. Ross']), 'Aristotle, 6th ed')

    def test_series_is_stripped(self):
        self.assertEqual(bmcr.strip_series('Sabina Augusta: An Imperial Journey. Women in antiquity'),
                         'Sabina Augusta: An Imperial Journey')
        self.assertEqual(bmcr.strip_series('Luxuria. Storie di banchetti, ville e altri eccessi'),
                         'Luxuria. Storie di banchetti, ville e altri eccessi')

    def test_two_digit_year_prefix(self):
        self.assertEqual(bmcr.clean_title('98.1.21, The Politics of Aristotle', ['Peter Simpson']),
                         'The Politics of Aristotle')

    def test_date(self):
        self.assertEqual(bmcr.parse_date('BMCR 2019.11.42'), '2019-11-01')


class TestFilter(unittest.TestCase):
    def rec(self, title, authors=('Jane Doe',)):
        return {'book_title': title, 'book_author_last_name': authors[0].split()[-1], '_authors': list(authors)}

    def test_keywords(self):
        for t in ('Aristotle on Desire', 'Faire usage: la pratique du stoïcisme', 'Plotinus on Beauty and Reality'):
            self.assertTrue(bmcr.is_philosophy(self.rec(t), {}), t)

    def test_classics_without_philosophy_is_dropped(self):
        for t in ('Latin inscriptions from the legionary fortress of Apulum', 'Cicero and the Roman Republic',
                  'Seneca: Phaedra'):
            self.assertFalse(bmcr.is_philosophy(self.rec(t), {}), t)

    def test_book_already_indexed(self):
        idx = {'long': [bmcr._title_words('Greek Models of Mind and Self')]}
        self.assertTrue(bmcr.is_philosophy(self.rec('Greek Models of Mind and Self', ('A. A. Long',)), idx))
        self.assertFalse(bmcr.is_philosophy(self.rec('Greek Vases in the Getty', ('A. A. Long',)), idx))


if __name__ == '__main__':
    unittest.main()
