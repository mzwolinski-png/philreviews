"""Regression tests for the extracted Crossref title/author parser."""
import os, sys, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import crossref_parsing as cp


class BibCitationFormat(unittest.TestCase):
    """JAP-style 'Title. Author, Year. Publisher. pp, price' citations
    (Wiley's non-italic review format) must parse title+author cleanly."""
    def test_parses_title_and_author(self):
        r = cp.parse_review_title(
            "Consent Matters. R.E.Goodin, 2024. Oxford, Oxford University Press. xiv + 255 pp, $110.00 (hb)")
        self.assertEqual(r["format"], "bib_citation")
        self.assertEqual(r["book_title"], "Consent Matters")
        self.assertEqual((r["book_author_first"], r["book_author_last"]), ("R. E.", "Goodin"))

    def test_title_ending_in_question_mark(self):
        r = cp.parse_review_title(
            "What's Wrong with Stereotyping?E.Beeghly, 2025. Oxford, Oxford University Press. ix + 249 pp, £30 (hb)")
        self.assertEqual(r["book_title"], "What's Wrong with Stereotyping?")
        self.assertEqual(r["book_author_last"], "Beeghly")

    def test_does_not_hijack_by_author_format(self):
        # Heythrop-style ". By Author. Pp." must NOT be claimed by the citation parser
        r = cp.parse_review_title(
            "Aristotle & the Virtues. By Howard J. Curzer. Pp. 451, Oxford University Press, 2012, $33.15.")
        self.assertNotEqual((r or {}).get("format"), "bib_citation")

    def test_no_garble_returns_none_not_swap(self):
        # a title with a publisher but no clean name-author split -> None, never a swap
        self.assertIsNone(cp._parse_bib_citation(
            "Some Article With Oxford University Press mentioned, 320 pp"))


class WileyByCitation(unittest.TestCase):
    """Bioethics-style 'Title By Last, First, City: Publisher, pp/ISBN'."""
    def test_inverted_single_author(self):
        r = cp.parse_review_title(
            "Gender Identity: What It Is and Why It Matters By Cosker‐Rowland, "
            "Rach, Oxford, UK: Oxford University Press, 368 pp. $40. ISBN: 978-0")
        self.assertEqual(r["format"], "wiley_by_citation")
        self.assertEqual(r["book_title"], "Gender Identity: What It Is and Why It Matters")
        self.assertEqual((r["book_author_first"], r["book_author_last"]),
                         ("Rach", "Cosker-Rowland"))

    def test_glued_names_and_eds(self):
        r = cp.parse_review_title(
            "Unravelling MAiD in Canada: Euthanasia and Assisted Suicide as Medical "
            "Care. By Ramona Coelho, K.SonuGaind, and TrudoLemmens (eds), Montreal, "
            "Quebec: McGill‐Queen's University Press, 2026. 320 pp.")
        self.assertEqual(r["book_author_last"], "Lemmens")
        self.assertTrue(r["is_edited_volume"])
        self.assertIn("Sonu Gaind", r["book_author_first"])

    def test_glued_city_salvage(self):
        r = cp.parse_review_title(
            "Karl Marx and the Actualization of Philosophy By ChristophSchuringa"
            "Cambridge: Cambridge University Press, 2025. 205 pp.")
        self.assertEqual((r["book_author_first"], r["book_author_last"]),
                         ("Christoph", "Schuringa"))

    def test_heythrop_pp_format(self):
        r = cp.parse_review_title(
            "Aristotle & the Virtues. By Howard J. Curzer. Pp. 451, Oxford "
            "University Press, 2012, $33.15.")
        self.assertEqual(r["book_title"], "Aristotle & the Virtues")
        self.assertEqual(r["book_author_last"], "Curzer")

    def test_no_by_marker_returns_other_format(self):
        r = cp.parse_review_title("Plain Article About Oxford University Press History")
        self.assertNotEqual((r or {}).get("format"), "wiley_by_citation")


class EiPFormats(unittest.TestCase):
    """Essays in Philosophy: 'Review of' variants must parse; bare colon
    titles must NOT be author-split (EiP audit 2026-07-19)."""
    EIP = {"container-title": ["Essays in Philosophy"]}

    def test_bare_colon_title_not_split(self):
        r = cp.parse_review_title(
            "Silent Parties: A Problem for Liberalism?", crossref_data=self.EIP)
        self.assertIsNone(r)

    def test_review_of_eds_extracts_editor(self):
        r = cp.parse_review_title("Review of The Nature of Truth, ed. Michael P. Lynch")
        self.assertEqual(r["format"], "review_of_eds")
        self.assertEqual(r["book_title"], "The Nature of Truth")
        self.assertEqual(r["book_author_last"], "Lynch")
        self.assertTrue(r["is_edited_volume"])

    def test_review_of_multi_eds_joined_convention(self):
        r = cp.parse_review_title(
            "Review of The African Philosophy Reader, ed. P.H. Coetzee and A.P.J. Roux")
        self.assertEqual(r["book_author_first"], "P.H. Coetzee and A.P.J.")
        self.assertEqual(r["book_author_last"], "Roux")

    def test_prepositional_title_not_author_split(self):
        r = cp.parse_review_title(
            "Despairing about War: The Democratic Limits of Pessimism")
        self.assertIsNone(r)

    def test_title_by_author_still_works_for_eip(self):
        r = cp.parse_review_title(
            "How to Be Perfect, by Michael Schur", crossref_data=self.EIP)
        self.assertEqual(r["book_author_last"], "Schur")


class IsBookReview(unittest.TestCase):
    def test_page_count_heuristic(self):
        self.assertTrue(cp.is_book_review(
            {"title": ["Justice. 320 pp"], "type": "journal-article"}, "all"))

    def test_italic_only_skips_plain_article(self):
        self.assertFalse(cp.is_book_review(
            {"title": ["Plain Article"], "type": "journal-article"}, "italic_only"))


class ParseReviewTitle(unittest.TestCase):
    def test_italic_title_only(self):
        r = cp.parse_review_title("<i>A Theory of Justice</i>")
        self.assertEqual(r["book_title"], "A Theory of Justice")
        self.assertEqual(r["format"], "italic_title_only")
        self.assertTrue(r["needs_doi_scrape"])  # author looked up later

    def test_title_by_author(self):
        r = cp.parse_review_title("The Republic by Plato (review)")
        self.assertEqual(r["book_title"], "The Republic")
        self.assertEqual(r["book_author_last"], "Plato")
        self.assertEqual(r["format"], "title_by_author")

    def test_italic_tags_with_author(self):
        r = cp.parse_review_title("Smith, John. <i>Ethics</i>. OUP, 2020")
        self.assertEqual(r["book_title"], "Ethics")
        self.assertEqual(r["book_author_first"], "John")
        self.assertEqual(r["book_author_last"], "Smith")
        self.assertEqual(r["format"], "italic_tags")




class GluedAndPossessiveFixes(unittest.TestCase):
    """Weekly-review fixes 2026-07-26: Dialogue's glued title+author citations,
    and possessive splits that ate 'The <X> Companion to <Name>'s <Title>'."""

    def test_glued_citation(self):
        r = cp.parse_review_title(
            "Science and Hypothesis, Historical Essays on Scientific Methodology"
            "Larry Laudan Dordrecht: D. Reidel Publishing Company, 1981. Pp. x, 258")
        self.assertEqual(r["format"], "glued_citation")
        self.assertEqual(r["book_author_last"], "Laudan")
        self.assertNotIn("Reidel", r["book_title"])

    def test_glued_multi_city(self):
        r = cp.parse_review_title(
            "Common SenseLynd Forguson London and New York: Routledge, 1989. "
            "vi + 193 p., $42.00")
        self.assertEqual(r["book_title"], "Common Sense")
        self.assertEqual(r["book_author_last"], "Forguson")

    def test_article_title_not_taken_as_possessive_author(self):
        r = cp.parse_review_title(
            "The Cambridge Companion to Augustine's Sermons. Edited by AndrewHofer. "
            "Cambridge: Cambridge University Press, 2025. Pp. 364. £110.00 (HB)")
        self.assertEqual(r["book_title"],
                         "The Cambridge Companion to Augustine's Sermons")
        self.assertEqual(r["book_author_last"], "Hofer")
        self.assertTrue(r["is_edited_volume"])

    def test_genuine_possessive_still_parses(self):
        r = cp.parse_review_title("Alva Noë's Strange Tools: Art and Human Nature")
        self.assertEqual(r["book_author_last"], "Noë")
        self.assertEqual(r["book_title"], "Strange Tools: Art and Human Nature")



class AccessTypeFromLicense(unittest.TestCase):
    """Crossref's `license` field is not an OA signal — subscription publishers
    attach their standard terms URL there (found 2026-07-31)."""
    def setUp(self):
        from crossref_scraper import _access_type_from_license
        self.f = _access_type_from_license

    def test_subscription_terms_are_restricted(self):
        for url in ("https://www.cambridge.org/core/terms",
                    "http://onlinelibrary.wiley.com/termsAndConditions",
                    "https://www.springernature.com/gp/researchers/text-and-data-mining"):
            self.assertEqual(self.f({"license": [{"URL": url}]}), "Restricted", url)

    def test_creative_commons_is_open(self):
        self.assertEqual(
            self.f({"license": [{"URL": "http://creativecommons.org/licenses/by/4.0/"}]}),
            "Open")

    def test_open_wins_when_mixed_with_tdm_terms(self):
        self.assertEqual(self.f({"license": [
            {"URL": "https://www.springernature.com/gp/researchers/text-and-data-mining"},
            {"URL": "http://creativecommons.org/licenses/by/4.0/"}]}), "Open")

    def test_no_license_is_restricted(self):
        self.assertEqual(self.f({}), "Restricted")



class ParenCitation(unittest.TestCase):
    """Review of Politics style: '[Essay - ]Author: Book. (City: Publisher, Yr. Pp. N.)'"""
    def test_strips_publisher_parenthetical(self):
        r = cp.parse_review_title(
            "Claudia Leeb: Contesting the Far Right: A Psychoanalytic and Feminist "
            "Critical Theory Approach (New York: Columbia University Press, 2024. Pp. 336.)")
        self.assertEqual(r["format"], "paren_citation")
        self.assertEqual(r["book_title"],
                         "Contesting the Far Right: A Psychoanalytic and Feminist Critical Theory Approach")
        self.assertEqual(r["book_author_last"], "Leeb")

    def test_possessive_book_title_not_split(self):
        r = cp.parse_review_title(
            "Robin Douglass: Mandeville's Fable: Pride, Hypocrisy, and Sociability. "
            "(Princeton and Oxford: Princeton University Press, 2023. Pp. xvi, 249.)")
        self.assertEqual(r["book_title"], "Mandeville's Fable: Pride, Hypocrisy, and Sociability")
        self.assertEqual(r["book_author_last"], "Douglass")

    def test_multi_author_byline(self):
        r = cp.parse_review_title(
            "Gabriele Badano and Alasia Nuti: Politicizing Political Liberalism: On the "
            "Containment of Illiberal and Antidemocratic Views. (Oxford: Oxford University "
            "Press, 2024. Pp. 208.)")
        self.assertEqual(r["book_author_first"], "Gabriele Badano and Alasia")
        self.assertEqual(r["book_author_last"], "Nuti")

    def test_review_essay_prefix_stripped(self):
        r = cp.parse_review_title(
            "Consent, Fairness and Political Obligation - George Klosko: The Principle of "
            "Fairness and Political Obligation. (Lanham, MD: Rowman and Littlefield "
            "Publishers, Inc., 1992. Pp. 190.)")
        self.assertEqual(r["book_title"], "The Principle of Fairness and Political Obligation")
        self.assertEqual(r["book_author_last"], "Klosko")

    def test_translator_tail_dropped(self):
        r = cp.parse_review_title(
            "Armin Schäfer and Michael Zürn: The Democratic Regression: The Political Causes "
            "of Authoritarian Populism. Trans. Stephen Curtis (Cambridge, and Hoboken, NJ: "
            "Polity Press, 2023. Pp. 200.)")
        self.assertEqual(r["book_title"],
                         "The Democratic Regression: The Political Causes of Authoritarian Populism")

    def test_comma_byline_running_into_title_is_rejected(self):
        # "Ellis Sandoz, The Politics of Truth…" — the segment after the comma is a
        # title, not a co-author; must not be claimed.
        self.assertIsNone(cp._parse_paren_citation(
            "The Modern Crisis - Ellis Sandoz, The Politics of Truth and Other Untimely "
            "Essays: The Crisis of Civic Consciousness. (Columbia: University of Missouri "
            "Press, 1999. Pp. xv, 235.)"))



class ParenCitationCommaByline(unittest.TestCase):
    """'Author, Book Title: Subtitle. (pub)' — the comma is the author/title
    boundary, so a naive first-colon split swallows the book's main title into
    the byline (regression found 2026-08-05)."""

    def test_comma_boundary_not_treated_as_coauthor(self):
        r = cp.parse_review_title(
            "Tom Arnold-Forster, Walter Lippmann: An Intellectual Biography. "
            "(Princeton and Oxford: Princeton University Press, 2025. Pp. ix, 353.)")
        self.assertEqual(r["book_title"], "Walter Lippmann: An Intellectual Biography")
        self.assertEqual(r["book_author_first"], "Tom")
        self.assertEqual(r["book_author_last"], "Arnold-Forster")

    def test_subtitle_with_internal_commas_survives(self):
        r = cp.parse_review_title(
            "Edward G. Miller, Misalliance: Ngo Dinh Diem, the United States, and "
            "the Fate of South Vietnam. (Cambridge, MA: Harvard University Press, "
            "2013. Pp. 419.)")
        self.assertTrue(r["book_title"].startswith("Misalliance: Ngo Dinh Diem"))
        self.assertEqual(r["book_author_last"], "Miller")

    def test_genuine_multi_author_byline_unaffected(self):
        r = cp.parse_review_title(
            "Gabriele Badano and Alasia Nuti: Politicizing Political Liberalism: On "
            "the Containment of Illiberal and Antidemocratic Views. (Oxford: Oxford "
            "University Press, 2024. Pp. 208.)")
        self.assertEqual(r["book_author_first"], "Gabriele Badano and Alasia")
        self.assertEqual(r["book_author_last"], "Nuti")



class EditorBylineFormats(unittest.TestCase):
    """Utilitas / UCL Press citation shapes (found 2026-08-08)."""

    def test_eds_prefix_keeps_every_editor(self):
        r = cp.parse_review_title(
            "Philip Schofield, Tim Causer and Chris Riley (eds.), The Correspondence "
            "of Jeremy Bentham, Volume 14: Supplementary Letters (London: UCL Press, "
            "2026) pp. xxiv + 701.")
        self.assertEqual(r["format"], "eds_prefix")
        self.assertEqual(r["book_title"],
                         "The Correspondence of Jeremy Bentham, Volume 14: Supplementary Letters")
        self.assertEqual(r["book_author_first"], "Philip Schofield, Tim Causer and Chris")
        self.assertEqual(r["book_author_last"], "Riley")
        self.assertTrue(r["is_edited_volume"])

    def test_single_editor(self):
        r = cp.parse_review_title(
            "Kevin Hart (ed.), Counter-Experiences: Reading Jean-Luc Marion "
            "(Notre Dame: University of Notre Dame Press, 2007), pp. 456.")
        self.assertEqual(r["book_author_last"], "Hart")
        self.assertEqual(r["book_title"], "Counter-Experiences: Reading Jean-Luc Marion")

    def test_author_title_edited_by(self):
        # the named editor is apparatus; the author keeps the byline
        r = cp.parse_review_title(
            "Jeremy Bentham, Essays on Logic, Ethics, and Universal Grammar, Edited "
            "by Philip Schofield (London, UCL Press, 2025), pp. lxxvi + 519.")
        self.assertEqual(r["format"], "author_title_editedby")
        self.assertEqual(r["book_title"], "Essays on Logic, Ethics, and Universal Grammar")
        self.assertEqual((r["book_author_first"], r["book_author_last"]), ("Jeremy", "Bentham"))



class WileyGluedAuthorList(unittest.TestCase):
    """Bioethics: '<Title><FirstLast>, <FirstLast>, and <FirstLast>, City: Pub'
    — no 'by' marker and every name glued (found 2026-08-12)."""

    def test_all_four_authors_recovered(self):
        r = cp.parse_review_title(
            "Rethinking Conscientious Objection in Health CareAlbertoGiubilini, "
            "UdoSchuklenk, FrancescaMinerva, and JulianSavulescu, New York: Oxford "
            "University Press, 2025. 265 pp. ISBN: 978-0-19-778653-6")
        self.assertEqual(r["format"], "wiley_glued_authorlist")
        self.assertEqual(r["book_title"], "Rethinking Conscientious Objection in Health Care")
        self.assertEqual(r["book_author_last"], "Savulescu")
        for name in ("Alberto Giubilini", "Udo Schuklenk", "Francesca Minerva"):
            self.assertIn(name, r["book_author_first"])

    def test_does_not_hijack_the_by_format(self):
        r = cp.parse_review_title(
            "Gender Identity: What It Is and Why It Matters By Cosker-Rowland, Rach, "
            "Oxford, UK: Oxford University Press, 368 pp. $40. ISBN: 978-0")
        self.assertEqual(r["format"], "wiley_by_citation")

    def test_does_not_hijack_single_glued_name(self):
        r = cp.parse_review_title(
            "Common SenseLynd Forguson London and New York: Routledge, 1989. "
            "vi + 193 p., $42.00")
        self.assertEqual(r["format"], "glued_citation")

if __name__ == "__main__":
    unittest.main()


class SageDuplicatedCitation(unittest.TestCase):
    """Sage 'Book Review:' records that repeat the whole citation after the byline."""

    def _p(self, t):
        return cp.parse_review_title(
            t, crossref_data={'container-title': ['Political Theory']})

    def test_collapses_repeat_and_splits_byline(self):
        r = self._p('Book Review: Free Gifts. Capitalism and the Politics of Nature , by '
                    'Alyssa Battistoni Free Gifts. Capitalism and the Politics of Nature, '
                    'by BattistoniAlyssa, Princeton and Oxford: Princeton University Press, '
                    '2025, 328 pp.')
        self.assertEqual(r['book_title'],
                         'Free Gifts. Capitalism and the Politics of Nature')
        self.assertEqual(r['book_author_last'], 'Battistoni')

    def test_inverted_byline_is_flipped(self):
        r = self._p('Book Review: Protecting Democracy in Europe , by Theuns, Tom '
                    'Protecting Democracy in Europe, by TheunsTom. London: Hurst')
        self.assertEqual(r['book_title'], 'Protecting Democracy in Europe')
        self.assertEqual(r['book_author_first'], 'Tom')
        self.assertEqual(r['book_author_last'], 'Theuns')

    def test_short_title_repeat_still_collapses(self):
        r = self._p('Book Review: Power in the Anthropocene , by Lars Tønder Power in '
                    'the Anthropocene, by TønderLars. Edinburgh University Press')
        self.assertEqual(r['book_title'], 'Power in the Anthropocene')
        self.assertEqual(r['book_author_last'], 'Tønder')

    def test_plain_colon_by_unaffected(self):
        r = self._p('Book Review: A Theory of Justice, by John Rawls')
        self.assertEqual(r['book_title'], 'A Theory of Justice')
        self.assertEqual(r['book_author_last'], 'Rawls')

    def test_no_repeat_left_alone(self):
        t = ('Book Review: The Cambridge Companion to Augustine, by Eleonore Stump')
        self.assertEqual(self._p(t)['book_title'], 'The Cambridge Companion to Augustine')


class CitationByline(unittest.TestCase):
    """New Blackfriars-style headers: 'Title, by Author. Publisher, City, Year. pp.'"""

    def test_single_book(self):
        b = cp.parse_citation_byline(
            'From Anecdote to Experiment in Psychical Research, by R. H. Thouless. '
            'Routledge and Kegan Paul. 198 pp. £3.')
        self.assertEqual(len(b), 1)
        self.assertEqual(b[0]['book_title'],
                         'From Anecdote to Experiment in Psychical Research')
        self.assertEqual(b[0]['book_author_first'], 'R. H.')
        self.assertEqual(b[0]['book_author_last'], 'Thouless')

    def test_translator_does_not_leak_into_author(self):
        b = cp.parse_citation_byline(
            'God in Fragments, by Jacques Pohier, trans. by John Bowden. '
            'SCM Press, London 1985. £9.50 paper.')
        self.assertEqual(b[0]['book_author_last'], 'Pohier')

    def test_order_suffix_kept_on_surname(self):
        b = cp.parse_citation_byline(
            'Clerical Celibacy Under Fire, by E. SchillebeeckxO.P. Sheed and Ward, 1968.')
        self.assertEqual(b[0]['book_author_first'], 'E.')
        self.assertEqual(b[0]['book_author_last'], 'Schillebeeckx, O.P.')

    def test_author_list_keeps_commas(self):
        b = cp.parse_citation_byline(
            'Celtic Nationalism, by Owen Dudley Edwards, Gwynfor Evans, Loan Rhys '
            'and Hugh MacDiarmid. Routledge, London, 1968. 358 pp.')
        self.assertEqual(b[0]['book_author_last'], 'MacDiarmid')
        self.assertIn('Gwynfor Evans,', b[0]['book_author_first'])
        self.assertTrue(b[0]['has_multiple_authors'])

    def test_glued_initials(self):
        b = cp.parse_citation_byline(
            'Language, Sense and Nonsense, by G.P. Baker and P.M.S. Hacker. '
            'Basil Blackwell, Oxford, 1984.')
        self.assertEqual(b[0]['book_title'], 'Language, Sense and Nonsense')
        self.assertEqual(b[0]['book_author_last'], 'Hacker')

    def test_multi_book_dash_separator(self):
        b = cp.parse_citation_byline(
            'Martin Heidegger, by John Macquarrie. Lutterworth Press, London, 1968. '
            '62 pp. 6s. - Ludwig Wittgenstein, by W. D. Hudson. Lutterworth Press, '
            'London, 1968. 74 pp. 6s.')
        self.assertEqual([x['book_title'] for x in b],
                         ['Martin Heidegger', 'Ludwig Wittgenstein'])
        self.assertEqual(b[1]['book_author_last'], 'Hudson')

    def test_semicolons_inside_title_do_not_split(self):
        b = cp.parse_citation_byline(
            'Italy in The Making (Vol. 1 : 1815-1846; Vol. 2: 1846-1848; Vol. 3: 1848), '
            'by G. F.-H. Berkeley. Cambridge University Press, 1968.')
        self.assertEqual(len(b), 1)
        self.assertTrue(b[0]['book_title'].startswith('Italy in The Making (Vol. 1'))
        self.assertEqual(b[0]['book_author_last'], 'Berkeley')

    def test_edited_volume_flagged(self):
        b = cp.parse_citation_byline(
            'Celibacy: The Necessary Option, edited by George H. Frein. Herder, 1968.')
        self.assertTrue(b[0]['is_edited_volume'])
        self.assertEqual(b[0]['book_author_last'], 'Frein')

    def test_corporate_author_declined(self):
        b = cp.parse_citation_byline(
            'A Guide to Religious Teaching Through the Bible and Liturgy, '
            'by a Group of Educationalists. Sands and Co., 1967.')
        self.assertEqual(b, [])


class CitationBylineNoComma(unittest.TestCase):
    """Same header without the comma: 'Title by Author. Imprint, Year. pp.'"""

    def test_no_comma_byline(self):
        b = cp.parse_citation_byline(
            'Who is My Brother? by Theo Westow. Sheed and Ward. 1966. 118 pp. 13s. 6d.')
        self.assertEqual(b[0]['book_title'], 'Who is My Brother?')
        self.assertEqual(b[0]['book_author_last'], 'Westow')

    def test_imprint_after_comma_is_not_a_second_author(self):
        b = cp.parse_citation_byline(
            'Human Dignity in Contemporary Ethics by David G. Kirchhoffer, '
            'Teneo Press, 2013. 300 pp.')
        self.assertEqual(b[0]['book_author_first'], 'David G.')
        self.assertEqual(b[0]['book_author_last'], 'Kirchhoffer')

    def test_role_after_comma_is_not_a_second_author(self):
        b = cp.parse_citation_byline(
            'Authority in Crisis? An Anglican Response by Robert Runcie, '
            'Archbishop of Canterbury. SCM, 1988. 100 pp.')
        self.assertEqual(b[0]['book_author_last'], 'Runcie')

    def test_bare_order_suffix(self):
        b = cp.parse_citation_byline(
            'The Body in Context by Gareth Moore OP. SCM Press, 1992. 250 pp.')
        self.assertEqual(b[0]['book_author_last'], 'Moore, O.P.')

    def test_publisher_acronym_never_becomes_a_name(self):
        b = cp.parse_citation_byline(
            'Freedom and Obligation, by C.K. Barrett. SPCK, London, 1985. 130 pp.')
        self.assertEqual(b[0]['book_author_last'], 'Barrett')

    def test_title_containing_by_is_left_alone(self):
        # no imprint follows, so this must not be read as a byline
        self.assertEqual(cp.parse_citation_byline('Salvation by Grace Alone'), [])


class CitationBylineBoundaries(unittest.TestCase):
    """Where the byline ends: series notes, imprints and editor bylines."""

    def test_earliest_byline_wins_over_editor(self):
        b = cp.parse_citation_byline(
            'On the Constitution of the Church and State by S.T. Coleridge, '
            'edited by John Colmer. Routledge, 1976. 200 pp.')
        self.assertEqual(b[0]['book_author_last'], 'Coleridge')

    def test_edited_by_not_left_in_title(self):
        b = cp.parse_citation_byline(
            'Councils and Assemblies. (Studies in Church History, vol. 7.) '
            'Edited by G. J. Cuming and L. G. D. Baker. CUP, 1971. 300 pp.')
        self.assertEqual(b[0]['book_title'],
                         'Councils and Assemblies. (Studies in Church History, vol. 7.)')
        self.assertTrue(b[0]['is_edited_volume'])

    def test_semicolon_before_imprint(self):
        b = cp.parse_citation_byline(
            'The Catholic Question in English politics 1820 to 1830. '
            'by G. I. T. Machin; Clarendon Press, Oxford, 1964. 30s.')
        self.assertEqual(b[0]['book_author_first'], 'G. I. T.')
        self.assertEqual(b[0]['book_author_last'], 'Machin')

    def test_series_note_is_not_a_coauthor(self):
        b = cp.parse_citation_byline(
            'Dialectic in Practical Religion, ed. by E. R. Leach, Cambridge '
            'Papers in Social Anthropology No. 5. CUP, 1968. 200 pp.')
        self.assertEqual(b[0]['book_author_last'], 'Leach')

    def test_long_editor_list_kept(self):
        b = cp.parse_citation_byline(
            'Christian History and Interpretation, edited by W. R. Farmer, '
            'C. F. D. Moule and R. R. Niebuhr. CUP, 1967. 400 pp.')
        self.assertEqual(b[0]['book_author_last'], 'Niebuhr')
        self.assertIn('C. F. D. Moule', b[0]['book_author_first'])

    def test_bare_ed_marker(self):
        b = cp.parse_citation_byline(
            'Biblical Studies: The Medieval Irish Contribution, ed. Martin McNamara, '
            'Dominican Publications, Dublin, 1976. 200 pp.')
        self.assertEqual(b[0]['book_title'],
                         'Biblical Studies: The Medieval Irish Contribution')
        self.assertEqual(b[0]['book_author_last'], 'McNamara')
        self.assertTrue(b[0]['is_edited_volume'])
