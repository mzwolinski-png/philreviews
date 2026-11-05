import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

import expand_initials as ei


def fake(names):
    return lambda title, surnames: names


class ExpandInitials(unittest.TestCase):
    def test_single_author(self):
        self.assertEqual(ei.expand_row('Anti-Racism as Communism', 'P.', 'Gomberg',
                                       fake(['Paul Gomberg'])), ('Paul', 'Gomberg'))

    def test_two_authors_site_convention(self):
        self.assertEqual(
            ei.expand_row('The Philosophy of Love, Sex, and Relationships',
                          'L. Brunning and N.', 'McKeever',
                          fake(['Luke Brunning', 'Natasha McKeever'])),
            ('Luke Brunning and Natasha', 'McKeever'))

    def test_initials_must_fit(self):
        # a different Gomberg is not taken
        self.assertIsNone(ei.expand_row('X', 'P.', 'Gomberg', fake(['Robert Gomberg'])))

    def test_ambiguous_left_alone(self):
        self.assertIsNone(ei.expand_row('X', 'A.', 'Smith', fake(['Adam Smith', 'Anna Smith'])))

    def test_full_names_untouched(self):
        self.assertIsNone(ei.expand_row('X', 'Paul', 'Gomberg', fake(['Paul Gomberg'])))

    def test_lookup_still_initials_not_used(self):
        self.assertIsNone(ei.expand_row('X', 'A. W.', 'Moore', fake(['A. W. Moore'])))

    def test_partial_expansion_of_multi_author(self):
        self.assertEqual(
            ei.expand_row('X', 'L. Brunning and N.', 'McKeever', fake(['Luke Brunning'])),
            ('Luke Brunning and N.', 'McKeever'))

    def test_accented_surname(self):
        self.assertEqual(ei.expand_row('X', 'J.', 'Müller', fake(['Jan Muller'])), ('Jan', 'Müller'))

    def test_lookup_artifacts_rejected(self):
        self.assertIsNone(ei.expand_row('X', 'P. A.', 'Brunt', fake(['P. A Brunt'])))
        self.assertIsNone(ei.expand_row('X', 'P. J.', 'Rhodes',
                          fake(['P J ARISTOTLE translated with an introduction and notes by Rhodes'])))
        self.assertIsNone(ei.expand_row('X', 'B.', 'Kjos-Hanssen', fake(['Bjø Kjos-Hanssen'])))

    def test_middle_initial_kept(self):
        self.assertEqual(ei.expand_row('X', 'K. P.', 'Harden', fake(['K. Paige Harden'])),
                         ('K. Paige', 'Harden'))


if __name__ == '__main__':
    unittest.main()
