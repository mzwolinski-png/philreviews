"""Per-journal Crossref scraping config (extracted from
crossref_scraper.CrossrefReviewScraper.JOURNALS so journal tweaks are a
data edit, not a change to the scraper module).
"""

JOURNALS = {
    # --- Original journals ---
    # Category A: <i> tags with author before them
    'Ethics': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
    'Utilitas': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
    'Inquiry': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
    'Philosophy of Science': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
    'European Journal of Philosophy': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
    # Category C: "Title by Author (review)"
    'Journal of the History of Philosophy': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
    # Category E: "Title, by Author"
    'Australasian Journal of Philosophy': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
    # Category B: <i>Title</i> only — book author from OpenAlex
    'The Philosophical Review': {'crossref_parseable': False, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
    # Category D: generic "Book Review" — enriched via Semantic Scholar
    'Mind': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},
    # Category F/D mix: older entries have "Title - Author", newer are generic
    'The Philosophical Quarterly': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},

    # --- New journals ---
    # "Title, Author. Publisher, Year, pages." or "<i>Title</i>, by Author"
    'Economics and Philosophy': {'crossref_parseable': True, 'openalex_enrichable': True},
    # "Book Review: Title, written/edited by Author" or "Author, Title (Publisher)"
    'Journal of Moral Philosophy': {'crossref_parseable': True, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
    # "Title. By Author. (Publisher, Year.)"
    'Philosophy': {'crossref_parseable': True, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
    # "Book Review: <i>Title</i>, by Author" or "Book Review: Title"
    'Political Theory': {'crossref_parseable': True, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
    # "Title, Author, Publisher" or "Title. Par Author." (French/English)
    # Uses 'dialogue' mode: italic_only + ALL-CAPS author names (skips generic name heuristics
    # that cause false positives on regular articles)
    'Dialogue': {'crossref_parseable': True, 'openalex_enrichable': True, 'detection_mode': 'dialogue'},
    # "Author <i>Title</i>. (Publisher, Year)" or "Author. Title. Pp."
    'Religious Studies': {'crossref_parseable': True},
    # "<i>Title</i>" or "Title, by Author"
    'Faith and Philosophy': {'crossref_parseable': True, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
    # "<i>Title</i>" embedded in text — often no author parseable
    'British Journal for the History of Philosophy': {'crossref_parseable': False, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
    # Mixed: "<i>Title</i>" — often no author parseable
    'The Journal of Aesthetics and Art Criticism': {'crossref_parseable': False, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
    # Generic "BOOK REVIEWS" — needs Semantic Scholar enrichment
    'The British Journal of Aesthetics': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},
    # Generic "Book reviews" or "Book Review" — needs enrichment
    'History and Philosophy of Logic': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},
    # Many generic "Book reviews" — needs enrichment
    'International Journal for Philosophy of Religion': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},
    # Mixed formats, many generic — needs enrichment
    'Journal of Applied Philosophy': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},
    # Mixed: some "Author: Title", many not parseable — needs enrichment
    'Continental Philosophy Review': {'crossref_parseable': False, 'semantic_scholar_enrichable': True},
    # Mixed: "Author Title. City, Publisher" — too inconsistent
    'Hypatia': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},
    # "Author, Title" or generic "Book reviews" — needs enrichment for generic ones
    'The Journal of Value Inquiry': {'crossref_parseable': True, 'semantic_scholar_enrichable': True},
    # "Author: Title" or "Author. Title" or "Title by Author"
    'Environmental Ethics': {'crossref_parseable': True, 'openalex_enrichable': True},
    # "Review of Author's Title. Publisher..." or "Author's Title. Publisher..."
    'Erasmus Journal for Philosophy and Economics': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # "Review of Title, by Author" format — clean parsing
    'Ancient Philosophy': {
        'crossref_parseable': True,
        'detection_mode': 'italic_only',
    },
    # Dedicated review journal — Author, "Title" format; all entries are reviews
    'Philosophy in Review': {
        'crossref_parseable': True,
        'all_reviews': True,
    },

    # --- Journals added via italic_only detection ---
    # Article titles commonly use colons/subtitles, so name-based heuristics cause false positives.
    # "Author <i>Title</i>" or "<i>Title</i>. By Author" format
    'The British Journal for the Philosophy of Science': {
        'crossref_parseable': False, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Generic "Book Review"/"Review" titles — needs Semantic Scholar enrichment
    'Erkenntnis': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Dedicated book review journal — all entries are reviews
    # Uses italic tags + "- By Author" format; many plain title-only entries
    'Philosophical Books': {
        'crossref_parseable': False, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # "Title by Author (review)" or "Title (review)" format
    'Philosophy East and West': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # "Title by Author (review)" format — near-perfect parsing
    'The Review of Metaphysics': {
        'crossref_parseable': True,
        'detection_mode': 'italic_only',
    },
    # "Title (review)" format — title-only, authors via OpenAlex
    'Philosophy and Literature': {
        'crossref_parseable': False, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Philosophy journal with reviews — mainly italic + (review) format
    'Sophia': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # --- Economics / PPE journals ---
    # "Book Review: Title" format (newer), "Book reviews" generic (older)
    'Quarterly Journal of Austrian Economics': {
        'crossref_parseable': True, 'openalex_enrichable': True,
    },
    # "Book Review: Title" and italic tags — small Crossref footprint (43 DOIs)
    'Journal of Libertarian Studies': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # "<i>Title</i> by Author" format — standard italic detection
    'History of Political Economy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # "Author, Title. City: Publisher, Year. Pages. Price" format
    'The Review of Austrian Economics': {
        'crossref_parseable': True, 'openalex_enrichable': True,
    },
    # "Title, by Author. Publisher, Year. Pages." format
    'Business Ethics Quarterly': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    'Journal of Moral Philosophy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
    },
    'Ethical Theory and Moral Practice': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # "BOOK REVIEW: Author. TITLE. Publisher, Year." format
    'Hypatia': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # "Author, Title. City: Publisher, Year, ISBN" format
    'Hypatia Reviews Online': {
        'crossref_parseable': True, 'openalex_enrichable': True,
    },

    # --- Tier 1: New journals (Feb 2026) ---
    # Generic "Book Review" titles — needs Semantic Scholar enrichment
    'Law and Philosophy': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # "Book review of Author's Title" — excellent descriptive titles
    'Phenomenology and the Cognitive Sciences': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # "Book Review: Title" format — parseable with Format R
    'Philosophy of the Social Sciences': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # "Book Review: Title" format
    'European Journal of Political Theory': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # --- Tier 2: New journals (Feb 2026) ---
    # Generic "Book Review" titles — needs Semantic Scholar enrichment
    'Bioethics': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # "Review Essay" and review format — use italic detection
    'The Review of Politics': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Generic "Book Review" titles — needs Semantic Scholar enrichment
    'Studies in History and Philosophy of Science': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Generic "Book Review" titles — needs Semantic Scholar enrichment
    'Philosophical Psychology': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Generic "Book Review" titles — needs Semantic Scholar enrichment
    'Public Choice': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # --- Non-Western philosophy ---
    # Generic "Book review" titles — needs Semantic Scholar enrichment
    'Journal of Indian Philosophy': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Generic "Book Reviews" / "Book review" titles
    'Asian Philosophy': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Generic "Book Reviews" titles
    'Dao': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # --- General philosophy (niche) ---
    # Generic "Book Review" titles
    'Metaphilosophy': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Generic "Book review" titles
    'Philosophia': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # --- Additional niche journals ---
    # Generic "Book Review" — large backlog from 1900s-1970s
    'The Monist': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Generic "Book Review Essay" / "Book Review" — education/ethics
    'Journal of Moral Education': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Mixed: "Title. By Author: Publisher, Year. pp." and "Book Received" — Scandinavian.
    # italic_only: Theoria publishes mostly research articles; their colon/comma
    # subtitles otherwise trip the generic name heuristics and import as fake reviews.
    'Theoria': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Generic "Book Review" — AI/philosophy of mind
    'Minds and Machines': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Generic "Book Review"
    'Ratio': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # "Book Review: Title" or "Review of Title" — some parseable
    'Res Publica': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Generic "BOOK REVIEW" / "Book Review"
    'Philosophical Investigations': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Generic "BOOK REVIEWS"
    'Journal of Social Philosophy': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Generic "Book Review" — medieval philosophy
    'Vivarium': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Book reviews and critical notices — American pragmatism
    'Transactions of the Charles S. Peirce Society': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── History of Philosophy ─────────────────────────────────────
    # "Author. <i>Title</i>" format — history of philosophy (est. ~842 reviews)
    'Archiv für Geschichte der Philosophie': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Philosophy of Mathematics ─────────────────────────────────
    # "Author. <i>Title</i>" format — philosophy of math (est. ~568 reviews)
    'Philosophia Mathematica': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Logic / Epistemology ──────────────────────────────────────
    # "Author. <i>Title</i>" format — logic and epistemology (est. ~454 reviews)
    'Dialectica': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Continental / General Philosophy ──────────────────────────
    # "<i>Title</i>, by Author" format — continental/general (est. ~121 reviews)
    'International Journal of Philosophical Studies': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── European Philosophy ───────────────────────────────────────
    # "Author. <i>Title</i>" format — European analytic philosophy (est. ~40 reviews)
    'Grazer Philosophische Studien': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── 17th/18th Century Philosophy ──────────────────────────────
    # "<b>Author</b>: Title" format (bold normalized to italic) — Kant scholarship
    'Kant-Studien': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Leibniz Review — ISSN not indexed in Crossref, skipped

    # ── 19th Century / Continental Philosophy ─────────────────────
    # Nietzsche scholarship (est. ~68 reviews)
    'The Journal of Nietzsche Studies': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Hegel scholarship (est. ~45 reviews)
    'Hegel Bulletin': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Continental/American philosophy (est. ~46 reviews)
    'The Journal of Speculative Philosophy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Philosophy of Language ────────────────────────────────────
    # Mind and language (est. ~85 reviews)
    'Mind &amp; Language': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Applied Ethics ────────────────────────────────────────────
    # Global bioethics (est. ~28 reviews)
    'Developing World Bioethics': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── African Philosophy ────────────────────────────────────────
    # Filosofia Theoretica — too few reviews in Crossref (~2), skipped
    # East African philosophy (est. ~10 reviews)
    'Thought and Practice': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── General Analytic Philosophy ───────────────────────────────
    # Analytic philosophy (est. ~19 reviews)
    'Acta Analytica': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── General / Broad Coverage ─────────────────────────────────
    # Southern Journal of Philosophy (est. ~181 reviews)
    'The Southern Journal of Philosophy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Philosophical Forum (est. ~59 reviews)
    'The Philosophical Forum': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Philosophy of Education ──────────────────────────────────
    # Studies in Philosophy and Education (est. ~151 reviews)
    'Studies in Philosophy and Education': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Journal of Philosophy of Education (est. ~82 reviews)
    'Journal of Philosophy of Education': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Educational Philosophy and Theory (est. ~134 reviews)
    'Educational Philosophy and Theory': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Continental Philosophy (more) ─────────────────────────────
    # Journal of the British Society for Phenomenology (est. ~143 reviews)
    'Journal of the British Society for Phenomenology': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Ancient Philosophy ────────────────────────────────────────
    # Apeiron — ancient Greek philosophy (est. ~76 reviews)
    'Apeiron': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Political Philosophy ──────────────────────────────────────
    # Critical Review of Intl Social and Political Philosophy (est. ~58 reviews)
    'Critical Review of International Social and Political Philosophy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Philosophy of Science ─────────────────────────────────────
    # Foundations of Science (est. ~35 reviews)
    'Foundations of Science': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Aesthetics ───────────────────────────────────────────────
    # British Journal of Aesthetics (est. ~192 reviews)
    'The British Journal of Aesthetics': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Social / Political Philosophy (more) ─────────────────────
    # Philosophy and Social Criticism (est. ~225 reviews)
    'Philosophy &amp; Social Criticism': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Constellations (est. ~34 reviews)
    'Constellations': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Philosophy of Religion (more) ─────────────────────────────
    # Neue Zeitschrift für Systematische Theologie (est. ~141 reviews)
    'Neue Zeitschrift für Systematische Theologie und Religionsphilosophie': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── History of Philosophy (more) ──────────────────────────────
    # Intellectual History Review (est. ~113 reviews)
    'Intellectual History Review': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Hume Studies (est. ~91 reviews)
    'Hume Studies': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Studia Leibnitiana (est. ~11 reviews)
    'Studia Leibnitiana': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Applied Ethics (more) ─────────────────────────────────────
    # Science and Engineering Ethics (est. ~68 reviews)
    'Science and Engineering Ethics': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── General Philosophy ────────────────────────────────────────
    # Synthese (est. ~104 reviews)
    'Synthese': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── History of Concepts ───────────────────────────────────────
    # Archiv für Begriffsgeschichte — ISSN maps to Philologus, skipped
    # History of Philosophy & Logical Analysis — ISSN maps to wrong journal, skipped

    # ── Analytic Philosophy ───────────────────────────────────────
    # Analysis (est. ~354 reviews)
    'Analysis': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Canadian Journal of Philosophy (est. ~126 reviews)
    'Canadian Journal of Philosophy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Feminist Philosophy / Bioethics ───────────────────────────
    # International Journal of Feminist Approaches to Bioethics (est. ~122 reviews)
    'IJFAB: International Journal of Feminist Approaches to Bioethics': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Process Philosophy ────────────────────────────────────────
    # Process Studies (est. ~95 reviews)
    'Process Studies': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── Pragmatism ────────────────────────────────────────────────
    # European Journal of Pragmatism and American Philosophy (est. ~76 reviews)
    'European Journal of Pragmatism and American Philosophy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── New journals (Feb 27 2026) ──────────────────────────────

    # Philosophy of Biology — NOT previously configured, ~64 reviews on Crossref
    'Biology and Philosophy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
    },
    # Phenomenology — ~66 reviews, "Book review" prefix format
    'Husserl Studies': {
        'crossref_parseable': True, 'openalex_enrichable': True,
    },
    # Philosophy of Law — ~72 reviews, italic tag format
    'Oxford Journal of Legal Studies': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # General philosophy — ~38 reviews, "Book Notices" format
    'International Philosophical Quarterly': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Philosophy of Law — ~15 reviews
    'Ratio Juris': {
        'crossref_parseable': True, 'openalex_enrichable': True,
    },
    # Topoi — removed: Crossref metadata unreliable (>95% false positive rate)
    # General M&E — top journal, few reviews but important
    'Noûs': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Logic — few reviews but fills gap
    'Journal of Philosophical Logic': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },

    # ── New journals (Feb 27 2026, batch 2) ─────────────────────

    # Environmental philosophy — "Book Review: <i>Title</i>" format (~765 est.)
    'Environmental Values': {
        'crossref_parseable': True, 'openalex_enrichable': True,
    },
    # Theology/philosophy of religion — "Title. By Author. Publisher, Year" format
    # Use italic_only to block possessive/colon false positives; ". By " pattern still fires
    'The Heythrop Journal': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Leibniz scholarship — "Review of Author, Title. Publisher, Year" format (~143 est.)
    'The Leibniz Review': {
        'crossref_parseable': True, 'openalex_enrichable': True,
    },
    # Legal philosophy — "Author, <i>Title</i>" format (~50 est.)
    'Jurisprudence': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Ethics — "Book Reviews" section (~75 est.)
    'Ethical Perspectives': {
        'crossref_parseable': True, 'openalex_enrichable': True,
    },
    # Social epistemology — mixed formats (~437 est.)
    'Social Epistemology': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Logic — "Author, Title. Publisher, Year" format (~229 est. new beyond 29 in DB)
    'Studia Logica': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Neuroethics — mixed formats (~131 est. new beyond 9 in DB)
    'Neuroethics': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Ancient/classical philosophy — some reviews (~32 est. new beyond 140 in DB)
    'Phronesis': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Agricultural/environmental ethics — mixed formats (~280 est.)
    'Journal of Agricultural and Environmental Ethics': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Ethics — "Author, <i>Title</i>" format (~152 est.)
    'The Journal of Ethics': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Legal philosophy — mixed (~52 est.)
    'Legal Theory': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Consciousness studies — mixed (~84 est.)
    'Journal of Consciousness Studies': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Social Philosophy and Policy — does NOT publish book reviews, skipped

    # --- Expansion (2026-02-28) ---

    # Thomistic/medieval philosophy — "Title by Author (review)" format (~2,200 est.)
    'The Thomist: A Speculative Quarterly Review': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Political science reviews — "Book Review: Category: Title" or italic tags (~500-800 est.)
    'Political Studies Review': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Dedicated review journal for HPS — subtitle has "Author: Title. Publisher, Year" (~2,400 est.)
    'Metascience': {
        'crossref_parseable': True,
        'all_reviews': True,
        'allow_author_replies': True,  # book authors contribute replies in symposia
        'symposium_detection': 'cluster',  # auto-group 3+ reviews of same book
    },
    # Political theory reviews — subtitle has "Author, Publisher, Year, ISBN" (~500-700 est.)
    'Contemporary Political Theory': {
        'crossref_parseable': True, 'openalex_enrichable': True,
    },
    # Intellectual history — generic "Book review" titles (~87 est.)
    'History of European Ideas': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
        'allow_author_replies': True,  # book authors contribute replies in symposia
        'symposium_detection': 'cluster',
    },

    # ── New journals from PI import (Mar 2026) ────────────────────

    # Teaching philosophy — "Title, by Author" format (~1,600 reviews, very active)
    'Teaching Philosophy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
    },
    # Top generalist — "Review of <i>Title</i>" or "Review of Author, Title" format
    'Philosophy and Phenomenological Research': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Top generalist — "Review of Author: Title" format. italic_only: most
    # JoP items are research articles, not reviews; only flagged book-citing
    # records (italic title / bib markers) should qualify.
    'The Journal of Philosophy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # History of philosophy of science — italic tags with full biblio info
    'HOPOS: The Journal of the International Society for the History of Philosophy of Science': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Kant scholarship — italic tags, some reviews detectable
    'Kantian Review': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Phenomenology — "Review Articles" section, Brill publisher
    'Research in Phenomenology': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # General philosophy of science — "BOOK REVIEW" generic titles
    'Journal for General Philosophy of Science': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Argumentation theory — "Book review" generic titles
    'Argumentation': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Rhetoric/philosophy — reviews with various formats
    'Philosophy & Rhetoric': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Criminal law/philosophy — "Review of Author, Title" or "Review of Author: Title" format
    'Criminal Law and Philosophy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Dedicated review journal — all items are reviews, "Author, Title" format
    'Phenomenological Reviews': {
        'all_reviews': True, 'crossref_parseable': True, 'openalex_enrichable': True,
    },
    # Theology/philosophy — "Title by Author. Publisher" or "Title. Publisher" format
    'New Blackfriars': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Continental philosophy of religion — "Title, by Author" or "Critical Study of Author, Title"
    'Journal for Continental Philosophy of Religion': {
        'crossref_parseable': True, 'openalex_enrichable': True,
    },

    # ── Previously unscanned journals (Apr 2026) ─────────────────────

    # Interdisciplinary humanities — "Author. <i>Title</i>" format (~484 est.)
    'Critical Inquiry': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Political philosophy — ":<i>Title</i>" and "Author. <i>Title</i>" format (~427 est.)
    'American Political Thought': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Aesthetics — "Author (Year) <i>Title</i>" format (~413 est.)
    'Film-Philosophy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Philosophy of religion — "Title. By Author. Publisher, Year" format (~374 est.)
    'Modern Theology': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # History of biology — mixed: "Book Review" generic + parseable titles (~295 est.)
    'Journal of the History of Biology': {
        'crossref_parseable': True, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Hastings — many articles use italicized subtitles ("Main: <i>Subtitle</i>"),
    # so italic_only mode produces lots of false positives. Require explicit bib
    # markers (ISBN, Pp., "by Author Name", or "(review)").
    'Hastings Center Report': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'bib_required',
    },
    # Philosophy of education — "Book review: Author, <i>Title</i>" format (~171 est.)
    'Theory and Research in Education': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # History of philosophy — "Author, <i>Title</i>" format (~160 est.)
    'Journal of Scottish Philosophy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Continental/phenomenology — mixed: "Book Reviews" generic + "Author. Title" (~156 est.)
    'Journal of Phenomenological Psychology': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Eastern European philosophy — "Review of Author, Title" format (~102 est.)
    'Studies in East European Thought': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Logic — italic format, reviews of books in mathematical logic (~93 est.)
    'The Bulletin of Symbolic Logic': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Continental — "Author, <i>Title</i>" format (~77 est.)
    'Derrida Today': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Utopian/political theory — italic format (~75 est.)
    'Utopian Studies': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # History/philosophy of biology — "Author, Title. Publisher, Year" format (~68 est.)
    'History and Philosophy of the Life Sciences': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Continental — "Book Review" / "Book Reviews" generic titles (~64 est.)
    'Sartre Studies International': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Thomistic/scholastic — "Title. By Author." format (~54 est.)
    'American Catholic Philosophical Quarterly': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Bioethics — "Author. Title. Publisher, Year" format (~49 est.)
    'Theoretical Medicine and Bioethics': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Philosophy of science — "Book Reviews" / "BOOK REVIEWS" generic (~46 est.)
    'International Studies in the Philosophy of Science': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Political/legal philosophy — "Author, Title. Publisher" format (~44 est.)
    'Human Rights Review': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Phenomenology — "Book review" generic titles (~33 est.)
    'Human Studies': {
        'crossref_parseable': False, 'semantic_scholar_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Continental — "BOOK REVIEW OF Author's Title" format (~28 est.)
    'Graduate Faculty Philosophy Journal': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Continental — "Author: <i>Title</i>" format (~26 est.)
    'Journal of Transcendental Philosophy': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
    # Ancient philosophy — italic format (~20 est.)
    'Ancient Philosophy Today': {
        'crossref_parseable': True, 'openalex_enrichable': True,
        'detection_mode': 'italic_only',
    },
}
