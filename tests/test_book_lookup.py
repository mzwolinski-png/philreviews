"""Book-author lookup from publishers' Crossref records (scripts/book_lookup.py)."""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import book_lookup as bl  # noqa: E402


class FakeResp:
    def __init__(self, items): self._items = items
    def json(self): return {"message": {"items": self._items}}


class FakeSession:
    def __init__(self, items): self.items = items
    def get(self, *a, **k): return FakeResp(self.items)


def book(title, sub=None, year=2025, author=None, editor=None):
    d = {"title": [title], "published": {"date-parts": [[year]]}}
    if sub: d["subtitle"] = [sub]
    if author: d["author"] = [{"given": g, "family": f} for g, f in author]
    if editor: d["editor"] = [{"given": g, "family": f} for g, f in editor]
    return d


class Lookup(unittest.TestCase):
    def test_exact_title_with_subtitle_returns_all_authors(self):
        s = FakeSession([book("Philosophical Mechanics in the Age of Reason", author=[("Katherine", "Brading"), ("Marius", "Stan")])])
        names, role = bl.crossref_book_people("Philosophical Mechanics in the Age of Reason", review_year=2025, session=s)
        self.assertEqual((names, role), (["Katherine Brading", "Marius Stan"], "author"))
        self.assertEqual(bl.author_fields(names), ("Katherine Brading and Marius", "Stan"))

    def test_editors_are_returned_for_an_edited_volume(self):
        s = FakeSession([book("Ernest Nagel", "Philosophy of Science and the Fight for Clarity",
                              editor=[("Matthias", "Neuber"), ("Adam Tamas", "Tuboly")])])
        names, role = bl.crossref_book_people("Ernest Nagel: Philosophy of Science and the Fight for Clarity",
                                              review_year=2025, session=s)
        self.assertEqual(role, "editor")
        self.assertEqual(names[-1], "Adam Tamas Tuboly")

    def test_a_near_match_is_not_a_match(self):
        s = FakeSession([book("Elites and Democracy in Latin America", author=[("A", "Other")])])
        self.assertEqual(bl.crossref_book_people("Elites and Democracy", review_year=2026, session=s), ([], None))

    def test_a_same_titled_book_from_another_era_is_refused(self):
        """'Philosophy of Education' (2026 review) must not get a 1996 book's editor."""
        s = FakeSession([book("Philosophy of Education", year=1996, editor=[("J. J.", "Chambliss")])])
        self.assertEqual(bl.crossref_book_people("Philosophy of Education", review_year=2026, session=s), ([], None))

    def test_the_reviewer_is_never_returned_as_the_author(self):
        s = FakeSession([book("A Long and Specific Enough Title", author=[("Jane", "Reviewer")])])
        self.assertEqual(bl.crossref_book_people("A Long and Specific Enough Title", "Reviewer",
                                                 review_year=2025, session=s), ([], None))

    def test_shouted_names_are_normalised(self):
        s = FakeSession([book("Freedom for All", "What a Liberal Society Could Be", author=[("ALEX", "ZAKARAS")])])
        names, _ = bl.crossref_book_people("Freedom for All: What a Liberal Society Could Be", review_year=2026, session=s)
        self.assertEqual(names, ["Alex Zakaras"])


if __name__ == "__main__":
    unittest.main()
