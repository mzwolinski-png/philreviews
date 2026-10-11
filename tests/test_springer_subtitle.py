"""The reviewed book from a Springer article page's subtitle (scripts/springer_subtitle.py).

Metascience headlines each review and prints the book beneath it; neither
Crossref nor the Springer Meta API carries that line. Each case below is a
real subtitle that an earlier version of the parser got wrong.
"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
from springer_subtitle import join_lines, parse_citation  # noqa: E402


class Citations(unittest.TestCase):
    def check(self, line, title, names, edited=False):
        self.assertEqual(parse_citation(line), (title, names, edited))

    def test_plain(self):
        self.check("Marc Lange: Because without cause: Non-causal explanation in science and mathematics. "
                   "Oxford: Oxford University Press, 2017, xxii+489pp, $74.00 HB",
                   "Because without cause: Non-causal explanation in science and mathematics", ["Marc Lange"])

    def test_editors(self):
        self.check("Franz Huber and Christoph Schmidt-Petri (eds): Degrees of belief. Dordrecht: Springer, 2008, 354 pp",
                   "Degrees of belief", ["Franz Huber", "Christoph Schmidt-Petri"], True)

    def test_inverted_first_editor(self):
        self.check("Wuppuluri, Shyam and A.C. Grayling (eds.): Metaphors and analogies in sciences and humanities: "
                   "words and worlds. Springer , 615 pp, 149.99 € HB",
                   "Metaphors and analogies in sciences and humanities: words and worlds",
                   ["Shyam Wuppuluri", "A.C. Grayling"], True)

    def test_initials_in_the_title_do_not_end_it(self):
        self.check("Matthew D. Lund: N. R. Hanson: Observation, discovery, and scientific change. Amherst, NY: "
                   "Humanity Books, 2010, 290pp, $18.98 PB",
                   "N. R. Hanson: Observation, discovery, and scientific change", ["Matthew D. Lund"])

    def test_a_year_in_the_title_does_not_end_it(self):
        self.check("Leslie Tomory: The history of the London water industry 1580–1820. Baltimore: Johns Hopkins "
                   "University Press, 314pp, $54.95 HB",
                   "The history of the London water industry 1580–1820", ["Leslie Tomory"])

    def test_question_mark_title_and_publisher_without_a_place(self):
        self.check("James Maclaurin and Kim Sterelny: What is Biodiversity? The University of Chicago Press, Chicago, "
                   "2008, xii + 217 pp, US$ 24 PB", "What is Biodiversity?", ["James Maclaurin", "Kim Sterelny"])

    def test_series_and_edition_are_not_part_of_the_title(self):
        self.check("Alisa Bokulich and Peter Bokulich (eds): Scientific structuralism. Boston studies in the philosophy "
                   "of science 281. Dordrecht: Springer, 2011, 200pp", "Scientific structuralism",
                   ["Alisa Bokulich", "Peter Bokulich"], True)
        self.check("Steven French: Philosophy of science: key concepts, 2nd edn. Bloomsbury Academic, 240pp, $ 24.95 PB",
                   "Philosophy of science: key concepts", ["Steven French"])

    def test_comma_before_the_place(self):
        self.check("Martin Kusch: Relativism in the philosophy of science, Cambridge: Cambridge University Press, 2020, 69 pp",
                   "Relativism in the philosophy of science", ["Martin Kusch"])

    def test_title_beginning_with_an_abbreviation(self):
        self.check("Lee Alan Dugatkin: Mr. Jefferson and the Giant Moose: natural history in early America. "
                   "The University of Chicago Press, Chicago, 2009, 184pp",
                   "Mr. Jefferson and the Giant Moose: natural history in early America", ["Lee Alan Dugatkin"])

    def test_elided_particle_in_a_surname(self):
        self.check("Bernard d’Espagnat: On physics and philosophy. Princeton and Oxford: Princeton University Press, "
                   "2013, xiv+503pp", "On physics and philosophy", ["Bernard d’Espagnat"])

    def test_citation_broken_across_two_lines(self):
        line = join_lines(["Nicholas P. Money", "The selfish ape: human nature and our path to extinction. "
                           "London: Reaktion Books, 2019, 152 pp"])
        self.check(line, "The selfish ape: human nature and our path to extinction", ["Nicholas P. Money"])

    def test_a_headline_is_not_a_citation(self):
        self.assertIsNone(parse_citation("The last Viennese polymath"))
        self.assertIsNone(parse_citation("Reply to my Critics: On explanations by constraint"))


if __name__ == "__main__":
    unittest.main()
