"""Unit tests for integrity_check pure helpers (no DB / network)."""
import os, sys, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import integrity_check as ic


class IsPersonName(unittest.TestCase):
    def test_plain_names(self):
        self.assertTrue(ic._is_person_name("John Rawls"))
        self.assertTrue(ic._is_person_name("Lynne Rudder Baker"))

    def test_rejects_title_punctuation(self):
        # commas/colons/semicolons mark a title fragment, never a name
        self.assertFalse(ic._is_person_name("The Legacy of Parmenides, Eleatic Monism"))
        self.assertFalse(ic._is_person_name("Justice: A Reader"))

    def test_rejects_wrong_word_count(self):
        self.assertFalse(ic._is_person_name("Springer"))            # 1 word
        self.assertFalse(ic._is_person_name("a b c d e"))           # 5 words

    def test_rejects_lowercase_nonparticle(self):
        self.assertFalse(ic._is_person_name("john rawls"))


class StripOrderSuffix(unittest.TestCase):
    def test_strips_op(self):
        self.assertEqual(ic._strip_order_suffix("Gregory O.P.", "Smith"),
                         ("Gregory", "Smith"))

    def test_particle_aware(self):
        # "de Lubac" surname must stay together after dropping the order suffix
        self.assertEqual(ic._strip_order_suffix("Henri", "de Lubac O.P."),
                         ("Henri", "de Lubac"))

    def test_excludes_cm_cp_initials(self):
        # documented gotcha: C.M./C.P. collide with middle initials -> never strip
        self.assertIsNone(ic._strip_order_suffix("Nancy C.M.", "Hartsock"))
        self.assertIsNone(ic._strip_order_suffix("J. C.P.", "Wright"))

    def test_no_suffix_no_change(self):
        self.assertIsNone(ic._strip_order_suffix("John", "Smith"))


if __name__ == "__main__":
    unittest.main()
