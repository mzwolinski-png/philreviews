"""The 'citation_required' detection mode, and shouted-title normalisation.

Journals that stop depositing italic tags fail silently: italic_only drops to
zero matches and the weekly run still reports success. Faith and Philosophy did
this around 2023 and went unnoticed for two years (found 2026-09-21).

'all' mode is not the remedy — measured across the affected journals it flags
ordinary article titles at roughly the same rate as real reviews. What actually
marks a review is that its title is a citation. These cases are the real titles
that motivated each rule, kept so the rule cannot be loosened by accident.
"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from crossref_parsing import is_book_review, looks_like_review_citation  # noqa: E402
from integrity_check import _is_shouting, titlecase_shouting  # noqa: E402


class ReviewCitations(unittest.TestCase):
    REVIEWS = [
        "The Problem of Evil for Atheists, by Yujin Nagasawa",
        "Christian Philosophy as a Way of Life: An Invitation to Wonder, by Ross Inman",
        "Alister McGrath, Christian Apologetics: An Introduction",
        "Eleonore Stump, The Image of God: The Problem of Evil and the Problem of Mourning",
        "Scott H. Moore, HOW TO BURN A GOAT: FARMING WITH THE PHILOSOPHERS",
        "Richard Cross, EARLY SCHOLASTIC CHRISTOLOGY 1050--1250",
        "The Sonic Gaze: Jazz, Whiteness, and Racialized Listening, written by T. Storm Heter",
        "Jewish Virtue Ethics, edited by Geoffrey D. Claussen, Alexander Green, and Alan L. Mittleman",
        "Book Review",
    ]
    # every one of these was flagged by 'all' mode on a journal we had marked quiet
    ARTICLES = [
        "Genome Engineering, Chemical Exposure, and the Germline: An Ethical Synthesis",
        "Climate Change, Shifting Nature, and Deliberation",
        "Hybrid Deference, Hybrid Chance",
        "Taking Risks, With and Without Probabilities",
        "Isaac Newton, Interdisciplinarian: Newton's cross-domain evidential reasoning",
        "Big Data, Machine Learning, and Personalization in Health Systems: Ethical Issues",
        "Philosophical Anthropology, Philosophical Technology, and Protocols of Intersubjectivity",
        "Terminal Boredom, Longevity, and Narrative",
        "Conspiracy Theories, Resistance to Evidence, and Propaganda: How Conspiracy Theories Advance",
        "Welfare, Connection, and Recognition",
    ]

    def test_citations_are_recognised(self):
        for t in self.REVIEWS:
            self.assertTrue(looks_like_review_citation(t), t)

    def test_article_titles_are_not(self):
        for t in self.ARTICLES:
            self.assertFalse(looks_like_review_citation(t), t)

    def test_co_editors_survive_the_name_capture(self):
        """A case-insensitive flag once let the capture swallow the joining 'and'."""
        t = ("John Henry Newman’s An Essay in Aid of a Grammar of Assent: "
             "A Critical Guide, edited by Frederick D. Aquino and Matthew Levering")
        self.assertTrue(looks_like_review_citation(t))

    def test_mode_is_wired_into_is_book_review(self):
        item = {"title": ["Alister McGrath, Christian Apologetics: An Introduction"]}
        self.assertTrue(is_book_review(item, "citation_required"))
        item = {"title": ["Climate Change, Shifting Nature, and Deliberation"]}
        self.assertFalse(is_book_review(item, "citation_required"))

    def test_the_two_journals_using_it_are_configured(self):
        from journals import JOURNALS
        for name in ("Faith and Philosophy", "Journal of Phenomenological Psychology"):
            self.assertEqual(JOURNALS[name]["detection_mode"], "citation_required")


class ShoutedTitles(unittest.TestCase):
    def test_all_caps_titles_are_folded(self):
        self.assertEqual(
            titlecase_shouting("PARFIT: A PHILOSOPHER AND HIS MISSION TO SAVE MORALITY"),
            "Parfit: A Philosopher and His Mission to Save Morality")
        self.assertEqual(titlecase_shouting("A MAP OF SELVES: BEYOND PHILOSOPHY OF MIND"),
                         "A Map of Selves: Beyond Philosophy of Mind")

    def test_ordinary_titles_are_left_alone(self):
        """Real acronyms must survive; only shouting is normalised."""
        for t in ("The AI Mirror: How to Reclaim Our Humanity",
                  "On What Matters", "GDP: A Brief but Affectionate History"):
            self.assertFalse(_is_shouting(t), t)

    def test_already_lowercase_words_are_not_capitalised(self):
        """'eds' after a caps author list is deliberate, not shouting."""
        self.assertEqual(titlecase_shouting("BROCK, STUART and ANTHONY EVERETT, eds"),
                         "Brock, Stuart and Anthony Everett, eds")

    def test_short_strings_are_not_treated_as_shouting(self):
        self.assertFalse(_is_shouting("GDP"))
        self.assertFalse(_is_shouting(""))


if __name__ == "__main__":
    unittest.main()
