"""A reply-word inside the reviewed book's own title must not veto the review."""
import os, sys, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from crossref_parsing import is_book_review


def _item(title, page="1218-1226"):
    return {"title": [title], "page": page, "type": "journal-article"}


class CitedTitleExclusions(unittest.TestCase):
    def test_response_in_book_title_is_a_review(self):
        self.assertTrue(is_book_review(_item(
            "<i>Fractured Goodness: Aristotle’s Response to Plato’s Form of the Good</i>\n"
            "                    , by Christopher Shields"), "italic_only"))

    def test_reply_outside_the_italics_is_still_excluded(self):
        self.assertFalse(is_book_review(_item(
            "Reply to Critics of <i>Fractured Goodness</i>"), "italic_only"))
        self.assertFalse(is_book_review(_item(
            "<i>Fractured Goodness</i>, by Christopher Shields: a response to Slabon"), "italic_only"))


if __name__ == "__main__":
    unittest.main()
