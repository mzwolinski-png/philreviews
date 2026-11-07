import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from crossref_parsing import fix_surname_initials as fix


class SurnameInitials(unittest.TestCase):
    def test_single(self):
        self.assertEqual(fix('Furedi', 'F'), ('F.', 'Furedi'))
        self.assertEqual(fix('Jung', 'JH'), ('J. H.', 'Jung'))

    def test_multi_author(self):
        self.assertEqual(fix('Mukherjee D and Anjum', 'T'), ('D. Mukherjee and T.', 'Anjum'))

    def test_roman_numerals_kept(self):
        self.assertEqual(fix('Edwards', 'III'), ('Edwards', 'III'))

    def test_normal_names_untouched(self):
        self.assertEqual(fix('David', 'Art'), ('David', 'Art'))
        self.assertEqual(fix('A.', 'Kaufman'), ('A.', 'Kaufman'))


if __name__ == '__main__':
    unittest.main()
