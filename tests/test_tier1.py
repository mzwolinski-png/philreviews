"""Unit tests for tier1_filter signal logic (no DB)."""
import os, sys, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import tier1_filter as t1


def _ref(titles=None, authors=None, reviewers=None):
    return {"titles": titles or {}, "authors": authors or set(),
            "reviewers": reviewers or set()}


class SignificantWords(unittest.TestCase):
    def test_drops_stopwords_and_short(self):
        w = t1._significant_words("The Theory of Justice")
        self.assertIn("theory", w)
        self.assertIn("justice", w)
        self.assertNotIn("the", w)
        self.assertNotIn("of", w)


class BookPassesFilter(unittest.TestCase):
    def test_signal1_title_overlap(self):
        ref = _ref(titles={"rawls": [{"theory", "justice"}]})
        self.assertTrue(t1.book_passes_filter(
            "Rawls", "A Theory of Justice", ref, publication_source="Quillette"))

    def test_signal1_requires_same_author(self):
        ref = _ref(titles={"rawls": [{"theory", "justice"}]})
        self.assertFalse(t1.book_passes_filter(
            "Nozick", "A Theory of Justice", ref, publication_source="Quillette"))

    def test_signal2_established_author_nonstrict(self):
        ref = _ref(authors={"macedo"})
        self.assertTrue(t1.book_passes_filter(
            "Macedo", "Some New Book", ref, publication_source="Reason"))

    def test_strict_source_ignores_signal2(self):
        # Critical Inquiry is STRICT -> author-only signal must not admit it
        ref = _ref(authors={"macedo"})
        self.assertFalse(t1.book_passes_filter(
            "Macedo", "Some New Book", ref, publication_source="Critical Inquiry"))

    def test_signal3_established_reviewer_nonstrict(self):
        ref = _ref(reviewers={t1._reviewer_key("Edward", "Feser")})
        self.assertTrue(t1.book_passes_filter(
            "Unknownauthor", "Unrelated Title", ref,
            reviewer_first_name="Edward", reviewer_last_name="Feser",
            publication_source="Claremont Review of Books"))


if __name__ == "__main__":
    unittest.main()
