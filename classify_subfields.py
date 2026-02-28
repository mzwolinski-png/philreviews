#!/usr/bin/env python3
"""
PhilReviews subfield classification.

Classifies all reviews into philosophical subfields using:
1. Journal-name mapping (~57% of entries)
2. Anthropic Batch API with Claude Haiku (~43% of entries)

Usage:
    python3 classify_subfields.py --journal-map-only    # Step 1: journal-based classification
    python3 classify_subfields.py --fetch-abstracts     # Step 2: fetch Crossref abstracts
    python3 classify_subfields.py --batch               # Step 3: submit batch API classification
    python3 classify_subfields.py --check-batch BATCH_ID  # Check batch status & download results
    python3 classify_subfields.py --stats               # Show classification stats
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
import urllib.request
import urllib.error

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("classify")

# ---------------------------------------------------------------------------
# Taxonomy
# ---------------------------------------------------------------------------
SUBFIELD_DISPLAY = {
    "ethics": "Ethics & Moral Philosophy",
    "applied-ethics": "Applied & Professional Ethics",
    "political": "Political & Social Philosophy",
    "legal": "Philosophy of Law",
    "epistemology": "Epistemology & Philosophy of Mind",
    "metaphysics": "Metaphysics & Logic",
    "science": "Philosophy of Science",
    "aesthetics": "Aesthetics & Philosophy of Art",
    "religion": "Philosophy of Religion & Theology",
    "history": "History of Philosophy",
    "ancient": "Ancient & Medieval Philosophy",
    "modern": "Early Modern Philosophy (17th-19th c.)",
    "continental": "Continental & Phenomenological",
    "feminist": "Feminist Philosophy",
    "non-western": "Non-Western & Comparative Philosophy",
}

VALID_SUBFIELDS = set(SUBFIELD_DISPLAY.keys())

# ---------------------------------------------------------------------------
# Journal → Subfield map
# ---------------------------------------------------------------------------
# Format: journal_name -> (primary, secondary_or_None)
# Covers specialist journals where the journal name uniquely determines subfield.
JOURNAL_SUBFIELD_MAP = {
    # Ethics & Moral Philosophy
    "Ethics": ("ethics", None),
    "International Journal of Ethics": ("ethics", None),
    "Ethical Theory and Moral Practice": ("ethics", None),
    "Journal of Moral Philosophy": ("ethics", None),
    "Utilitas": ("ethics", None),
    "The Journal of Value Inquiry": ("ethics", None),
    "The Journal of Religious Ethics": ("ethics", None),
    "Ethical Perspectives": ("ethics", None),
    "The Journal of Ethics": ("ethics", None),
    "Journal of Moral Education": ("ethics", None),
    "Social Theory and Practice": ("ethics", "political"),

    # Applied & Professional Ethics
    "Journal of Medical Ethics": ("applied-ethics", None),
    "Bioethics": ("applied-ethics", None),
    "Business Ethics Quarterly": ("applied-ethics", None),
    "Environmental Ethics": ("applied-ethics", None),
    "Environmental Values": ("applied-ethics", None),
    "Environmental Philosophy": ("applied-ethics", None),
    "Ethics and the Environment": ("applied-ethics", None),
    "Journal of Business Ethics": ("applied-ethics", None),
    "Business & Professional Ethics Journal": ("applied-ethics", None),
    "Kennedy Institute of Ethics Journal": ("applied-ethics", None),
    "Theoretical Medicine and Bioethics": ("applied-ethics", None),
    "Journal of Animal Ethics": ("applied-ethics", "ethics"),
    "Journal of Agricultural and Environmental Ethics": ("applied-ethics", None),
    "Ethics and Information Technology": ("applied-ethics", None),
    "Neuroethics": ("applied-ethics", None),
    "Cambridge Quarterly of Healthcare Ethics": ("applied-ethics", None),
    "Science and Engineering Ethics": ("applied-ethics", None),
    "IJFAB: International Journal of Feminist Approaches to Bioethics": ("applied-ethics", "feminist"),
    "Developing World Bioethics": ("applied-ethics", None),
    "Medicine, Health Care and Philosophy": ("applied-ethics", None),
    "Journal of Markets and Morality": ("applied-ethics", "political"),

    # Political & Social Philosophy
    "Political Theory": ("political", None),
    "The Review of Politics": ("political", None),
    "Economics and Philosophy": ("political", None),
    "Public Choice": ("political", None),
    "The Independent Review": ("political", None),
    "History of Political Economy": ("political", "history"),
    "American Political Thought": ("political", "history"),
    "Critical Review of International Social and Political Philosophy": ("political", None),
    "European Journal of Political Theory": ("political", None),
    "Philosophy & Social Criticism": ("political", None),
    "Journal of Social Philosophy": ("political", None),
    "Constellations": ("political", "continental"),
    "Res Publica": ("political", None),
    "The CLR James Journal": ("political", None),
    "The Black Scholar": ("political", None),
    "Constitutional Political Economy": ("political", "legal"),
    "Libertarian Papers": ("political", None),
    "Erasmus Journal for Philosophy and Economics": ("political", None),
    "Journal of Libertarian Studies": ("political", None),
    "Quarterly Journal of Austrian Economics": ("political", None),
    "The Review of Austrian Economics": ("political", None),
    "Reason Papers": ("political", None),

    # Philosophy of Law
    "Ratio Juris": ("legal", None),
    "Law and Philosophy": ("legal", None),
    "Jurisprudence": ("legal", None),
    "Oxford Journal of Legal Studies": ("legal", None),
    "Legal Theory": ("legal", None),
    "The Canadian Journal of Law and Jurisprudence": ("legal", None),

    # Epistemology & Philosophy of Mind
    "Philosophical Psychology": ("epistemology", None),
    "Minds and Machines": ("epistemology", None),
    "Mind & Language": ("epistemology", None),
    "Phenomenology and the Cognitive Sciences": ("epistemology", "continental"),
    "Social Epistemology": ("epistemology", None),
    "Social Epistemology Review and Reply Collective": ("epistemology", None),
    "Journal of Consciousness Studies": ("epistemology", None),
    "Behavior and Philosophy": ("epistemology", None),
    "Behaviorism": ("epistemology", None),

    # Metaphysics & Logic
    "Studia Logica": ("metaphysics", None),
    "Journal of Philosophical Logic": ("metaphysics", None),
    "The Bulletin of Symbolic Logic": ("metaphysics", None),
    "Philosophia Mathematica": ("metaphysics", None),
    "History and Philosophy of Logic": ("metaphysics", "history"),

    # Philosophy of Science
    "Philosophy of Science": ("science", None),
    "The British Journal for the Philosophy of Science": ("science", None),
    "The British Society for Philosophy of Science": ("science", None),
    "The British Society for the Philosophy of Science": ("science", None),
    "British Journal for Philosophy of Science": ("science", None),
    "The British Journal of Philosophy of Science": ("science", None),
    "Biology and Philosophy": ("science", None),
    "History and Philosophy of the Life Sciences": ("science", "history"),
    "Journal of the History of Biology": ("science", "history"),
    "Philosophy of the Social Sciences": ("science", None),
    "HOPOS: The Journal of the International Society for the History of Philosophy of Science": ("science", "history"),
    "Arabic Sciences and Philosophy": ("science", "non-western"),
    "The New Atlantis": ("science", None),

    # Aesthetics & Philosophy of Art
    "The Journal of Aesthetics and Art Criticism": ("aesthetics", None),
    "Philosophy and Literature": ("aesthetics", None),
    "The British Journal of Aesthetics": ("aesthetics", None),
    "Philosophy & Rhetoric": ("aesthetics", None),

    # Philosophy of Religion & Theology
    "Religious Studies": ("religion", None),
    "The Heythrop Journal": ("religion", None),
    "International Journal for Philosophy of Religion": ("religion", None),
    "Faith and Philosophy": ("religion", None),
    "American Journal of Theology & Philosophy": ("religion", None),
    "Sophia": ("religion", None),
    "Process Studies": ("religion", "metaphysics"),
    "Max Weber Studies": ("religion", "political"),
    "Journal of the Society of Christian Ethics": ("religion", "ethics"),

    # History of Philosophy (general)
    "Journal of the History of Philosophy": ("history", None),
    "British Journal for the History of Philosophy": ("history", None),
    "Archiv für Geschichte der Philosophie": ("history", None),
    "History of Philosophy and Logical Analysis": ("history", None),
    "History of Philosophy & Logical Analysis": ("history", None),
    "History of Philosophy and Legal Analysis": ("history", None),
    "Intellectual History Review": ("history", None),

    # Ancient & Medieval Philosophy
    "Phronesis": ("ancient", None),
    "Ancient Philosophy": ("ancient", None),
    "Vivarium": ("ancient", None),
    "Apeiron": ("ancient", None),

    # Early Modern Philosophy (17th-19th c.)
    "Kant-Studien": ("modern", None),
    "Studia Leibnitiana": ("modern", None),
    "The Leibniz Review": ("modern", None),
    "Hume Studies": ("modern", None),
    "Hegel Bulletin": ("modern", None),
    "Transactions of the Charles S. Peirce Society": ("modern", None),

    # Continental & Phenomenological
    "Research in Phenomenology": ("continental", None),
    "Continental Philosophy Review": ("continental", None),
    "Journal of the British Society for Phenomenology": ("continental", None),
    "Husserl Studies": ("continental", None),
    "Heidegger Studies": ("continental", None),
    "Human Studies": ("continental", None),
    "Sartre Studies International": ("continental", None),
    "The Journal of Nietzsche Studies": ("continental", "modern"),
    "European Journal of Pragmatism and American Philosophy": ("continental", "history"),
    "Radical Philosophy": ("continental", "political"),

    # Feminist Philosophy
    "Hypatia": ("feminist", None),
    "Hypatia Reviews Online": ("feminist", None),

    # Non-Western & Comparative Philosophy
    "Philosophy East and West": ("non-western", None),
    "Dao": ("non-western", None),
    "Journal of Indian Philosophy": ("non-western", None),
    "Journal of Chinese Philosophy": ("non-western", None),
    "Asian Philosophy": ("non-western", None),
    "Frontiers of Philosophy in China": ("non-western", None),
    "Comparative Philosophy": ("non-western", None),
    "Studies in East European Thought": ("non-western", "political"),
    "Studies in Soviet Thought": ("non-western", "political"),

    # Philosophy of Education
    "Studies in Philosophy and Education": ("applied-ethics", "political"),
    "Journal of Philosophy of Education": ("applied-ethics", "political"),
    "Educational Philosophy and Theory": ("applied-ethics", "political"),

    # Utopian Studies → Political
    "Utopian Studies": ("political", None),

    # Generalist journals with mild leanings — classify by journal as a fallback
    # These will still be sent to the LLM, but we provide a default
    # EXCLUDED from journal map — these need LLM:
    # Philosophy in Review, Notre Dame Philosophical Reviews, Philosophy,
    # The Philosophical Review, Dialogue, Mind, The Journal of Philosophy,
    # Philosophy and Phenomenological Research, Canadian Journal of Philosophy,
    # Australasian Journal of Philosophy, Noûs, European Journal of Philosophy,
    # Synthese, Analysis, Dialectica, Inquiry, etc.

    # Mainstream media outlets — these need LLM classification
    # (titles vary widely in topic)
}

# Journals that are "generalist" — must be classified by LLM
GENERALIST_JOURNALS = {
    "Philosophy in Review",
    "Notre Dame Philosophical Reviews",
    "Philosophy",
    "The Philosophical Review",
    "Dialogue",
    "Mind",
    "The Journal of Philosophy",
    "Philosophy and Phenomenological Research",
    "Canadian Journal of Philosophy",
    "Australasian Journal of Philosophy",
    "Noûs",
    "European Journal of Philosophy",
    "Synthese",
    "Analysis",
    "Dialectica",
    "Inquiry",
    "The Philosophical Quarterly",
    "Philosophical Studies",
    "International Journal of Philosophical Studies",
    "The Southern Journal of Philosophy",
    "Metaphilosophy",
    "Philosophia",
    "The Philosophical Forum",
    "The Personalist Forum",
    "The Pluralist",
    "International Philosophical Quarterly",
    "The Centennial Review",
    "Journal of Philosophical Studies",
    "Philosophical Books",
    "Philosophical Investigations",
    "The Review of Metaphysics",
    "The Journal of Speculative Philosophy",
    "Iyyun: The Jerusalem Philosophical Quarterly / עיון: רבעון פילוסופי",
    "Journal of Thought",
    "Topoi",
    "Erkenntnis",
    "Ratio",
    "Acta Analytica",
    "Grazer Philosophische Studien",
    "Theoria",
    "Crítica: Revista Hispanoamericana de Filosofía",
    "South African Journal of Philosophy",
    "Filosofia Theoretica",
    "Thought and Practice",
    "Cosmos + Taxis",
    "The Journal of Ayn Rand Studies",
    "The Southwestern Journal of Philosophy",
    "Critical Philosophy of Race",
    "Philosophy Now",
    "The Philosopher",
    "Philosopher's Magazine",
    "The Philosophers' Magazine",
    "Journal of Applied Philosophy",
}

# All mainstream media outlets need LLM classification
MAINSTREAM_OUTLETS = {
    "The Times Literary Supplement", "Los Angeles Review of Books",
    "The Wall Street Journal", "The Guardian", "The New York Review of Books",
    "London Review of Books", "Kirkus Reviews", "Boston Review",
    "The New York Times", "Literary Review", "Australian Book Review",
    "The Washington Post", "The New Yorker", "The Nation", "The Atlantic",
    "The Telegraph", "Times Higher Education", "The Hedgehog Review",
    "Commonweal", "The New Republic", "Sydney Review of Books",
    "Prospect Magazine", "The Spectator", "The New Statesman",
    "Inside Higher Ed", "The Times", "Publisher's Weekly",
    "The Economist", "The Boston Globe", "City Journal",
    "National Review", "Jacobin", "Bookforum", "The Critic",
    "The Financial Times", "The Baffler", "The New York Journal of Books",
    "The Oxonian Review", "San Francisco Book Review",
    "The Point Magazine", "The Point", "The Jewish Chronicle",
    "The Conversation", "The Christian Science Monitor", "The American Scholar",
    "Psychology Today", "Prospect", "New Rambler Review", "Liberal Currents",
    "LA Review of Books", "Jewish Review of Books", "Hyperallergic",
    "Harper's Magazine", "Harper's", "Dublin Review of Books",
    "Dissent Magazine", "Commonweal Magazine", "Arc Digital", "Arc",
    "Vox", "UnHerd", "Spiked", "Quillette", "Public Discourse",
    "Public Books", "Pop Matters", "Patheos", "New Scientist",
    "New Left Review", "National Public Radio", "NPR",
    "The Spectator Australia", "The Star", "The Stanford Daily",
    "The Roanoke Times", "The New Criterion", "The National Post",
    "The Monthly", "The Metropolitan Review", "The Los Angeles Times",
    "The London School of Economics Blog", "The Irish Times",
    "The Ideas Letter", "The Humanist", "The Hindu", "The Herald Scotland",
    "The Globe and Mail", "The Evening Standard", "The Dispatch",
    "The Daily Mail", "The Chronicle of Higher Education",
    "The Cambridge Humanities Review", "The Blog of the American Philosophical Association",
    "The Associated Press", "The Arts Fuse", "The Wire India",
    "The Sunday Times", "The Times of London", "Times Higher Ed",
    "Standpoint", "Shepherd Express", "Science", "Real Clear Defense",
    "Publishers' Weekly", "Oxonia", "Mid Theory Collective",
    "Irish Examiner", "India Today", "Harvard Magazine", "Gawker",
    "Fast Company", "Democratic Audit", "Damage Magazine", "Counterfire",
    "Commentary Magazine", "Cleveland Review of Books", "Classics for All",
    "Claremont Review of Books", "Church Times", "Booktrib",
    "Australian Broadcasting Corporation Radio", "ArtReview",
    "American Affairs", "America Magazine", "4Columns", "3:16 AM",
    "360 Magazine", "Washington Independent Review of Books",
    "Washington Examiner", "Understanding Society", "Teachers College Record",
    "The University Bookman", "Intercollegiate Studies Institute",
    "Inside Story", "National Business Review", "New Rambler Review",
    "The Humanis", "The New York Review",
}


def get_conn():
    return sqlite3.connect(db.DB_PATH)


# ---------------------------------------------------------------------------
# Step 1: Journal-map classification
# ---------------------------------------------------------------------------
def apply_journal_map(dry_run=False):
    """Classify entries based on journal name alone."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row

    # Count entries that could be classified
    total_mappable = 0
    already_classified = 0
    newly_classified = 0

    for journal, (primary, secondary) in JOURNAL_SUBFIELD_MAP.items():
        rows = conn.execute(
            "SELECT id, subfield_primary FROM reviews WHERE publication_source = ?",
            (journal,),
        ).fetchall()
        for row in rows:
            total_mappable += 1
            if row["subfield_primary"]:
                already_classified += 1
                continue
            if not dry_run:
                conn.execute(
                    "UPDATE reviews SET subfield_primary = ?, subfield_secondary = ? WHERE id = ?",
                    (primary, secondary, row["id"]),
                )
            newly_classified += 1

    if not dry_run:
        conn.commit()
    conn.close()

    log.info(f"Journal map: {total_mappable} mappable entries, "
             f"{already_classified} already classified, "
             f"{newly_classified} newly classified")
    return newly_classified


# ---------------------------------------------------------------------------
# Step 2: Fetch Crossref abstracts
# ---------------------------------------------------------------------------
def strip_jats(text):
    """Strip JATS XML tags from Crossref abstracts."""
    if not text:
        return text
    return re.sub(r"<[^>]+>", "", text).strip()


def fetch_crossref_abstracts(batch_size=50, rate_limit=0.05):
    """Fetch abstracts from Crossref for entries with DOIs but no review_summary."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT id, doi FROM reviews "
        "WHERE doi IS NOT NULL AND doi != '' "
        "AND (review_summary IS NULL OR review_summary = '') "
        "ORDER BY id"
    ).fetchall()

    log.info(f"Crossref abstract fetch: {len(rows)} entries with DOIs but no summary")

    fetched = 0
    errors = 0
    no_abstract = 0
    consecutive_errors = 0

    for i, row in enumerate(rows):
        doi = row["doi"]
        if not doi:
            continue

        # Clean DOI
        doi = doi.strip()
        if doi.startswith("https://doi.org/"):
            doi = doi[len("https://doi.org/"):]
        if doi.startswith("http://doi.org/"):
            doi = doi[len("http://doi.org/"):]

        url = f"https://api.crossref.org/works/{urllib.request.quote(doi, safe='')}"
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)")

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                abstract = data.get("message", {}).get("abstract", "")
                if abstract:
                    abstract = strip_jats(abstract)
                    if len(abstract) > 20:  # Skip trivially short ones
                        conn.execute(
                            "UPDATE reviews SET review_summary = ? WHERE id = ?",
                            (abstract, row["id"]),
                        )
                        fetched += 1
                    else:
                        no_abstract += 1
                else:
                    no_abstract += 1
                consecutive_errors = 0
        except (urllib.error.HTTPError, urllib.error.URLError, Exception) as e:
            errors += 1
            consecutive_errors += 1
            if consecutive_errors >= 50:
                log.warning("50 consecutive errors — stopping abstract fetch")
                break
            if errors % 100 == 0:
                log.info(f"  {errors} errors so far (last: {e})")

        # Rate limit
        time.sleep(rate_limit)

        # Progress logging
        if (i + 1) % 500 == 0:
            conn.commit()
            log.info(f"  Progress: {i + 1}/{len(rows)} — fetched {fetched}, "
                     f"no abstract {no_abstract}, errors {errors}")

    conn.commit()
    conn.close()
    log.info(f"Crossref abstracts: {fetched} fetched, {no_abstract} without abstract, "
             f"{errors} errors out of {len(rows)} DOIs")
    return fetched


# ---------------------------------------------------------------------------
# Step 3: Batch API classification
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You classify philosophy book reviews into subfields. Return JSON only.

Subfields: ethics, applied-ethics, political, legal, epistemology, metaphysics, science, aesthetics, religion, history, ancient, modern, continental, feminist, non-western

Rules:
- "primary": required, one subfield
- "secondary": optional, one subfield or null
- Use secondary only when the book clearly spans two fields
- "history" = general history of philosophy; "ancient" = ancient/medieval; "modern" = 17th-19th century
- "applied-ethics" = bioethics, business ethics, environmental ethics, medical ethics, tech ethics
- "epistemology" includes philosophy of mind, philosophy of language
- "metaphysics" includes logic, philosophy of mathematics
- "science" includes philosophy of biology, physics, social sciences
- "continental" = phenomenology, hermeneutics, existentialism, critical theory
- Classify by the BOOK's topic, not the journal"""


def build_user_message(title, journal, abstract=None):
    """Build the user message for a single classification request."""
    parts = [f"T: {title}"]
    if abstract:
        parts.append(f"A: {abstract[:200]}")
    parts.append(f"J: {journal}")
    return "\n".join(parts)


def submit_batch(limit=None):
    """Submit a batch of unclassified entries to the Anthropic Batch API."""
    try:
        import anthropic
    except ImportError:
        log.error("anthropic package not installed. Run: pip3 install anthropic")
        sys.exit(1)

    conn = get_conn()
    conn.row_factory = sqlite3.Row

    query = (
        "SELECT id, book_title, publication_source, review_summary "
        "FROM reviews WHERE subfield_primary IS NULL OR subfield_primary = '' "
        "ORDER BY id"
    )
    rows = conn.execute(query).fetchall()
    conn.close()

    if not rows:
        log.info("No unclassified entries remaining")
        return None

    if limit:
        rows = rows[:limit]

    log.info(f"Building batch for {len(rows)} entries...")

    requests = []
    for row in rows:
        title = row["book_title"] or ""
        journal = row["publication_source"] or ""
        abstract = row["review_summary"] or ""

        if not title and not journal:
            continue

        msg = build_user_message(title, journal, abstract if abstract else None)

        requests.append({
            "custom_id": str(row["id"]),
            "params": {
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 60,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": msg}],
            },
        })

    if not requests:
        log.info("No valid requests to submit")
        return None

    client = anthropic.Anthropic()

    # Submit in batches of 5,000 to stay within payload limits
    batch_ids = []
    for i in range(0, len(requests), 5000):
        chunk = requests[i:i + 5000]
        log.info(f"Submitting batch {i // 5000 + 1} with {len(chunk)} requests...")

        batch = client.messages.batches.create(requests=chunk)
        batch_ids.append(batch.id)
        log.info(f"  Batch ID: {batch.id} — status: {batch.processing_status}")

    return batch_ids


def check_and_apply_batch(batch_id):
    """Check batch status and apply results if complete."""
    try:
        import anthropic
    except ImportError:
        log.error("anthropic package not installed. Run: pip3 install anthropic")
        sys.exit(1)

    client = anthropic.Anthropic()

    batch = client.messages.batches.retrieve(batch_id)
    log.info(f"Batch {batch_id}: {batch.processing_status}")
    log.info(f"  Counts: {batch.request_counts}")

    if batch.processing_status != "ended":
        log.info("Batch not yet complete — check back later")
        return 0

    # Download and apply results
    conn = get_conn()
    applied = 0
    parse_errors = 0

    for result in client.messages.batches.results(batch_id):
        review_id = result.custom_id

        if result.result.type != "succeeded":
            log.warning(f"  Entry {review_id}: {result.result.type}")
            continue

        # Extract text from the response
        text = ""
        for block in result.result.message.content:
            if block.type == "text":
                text += block.text

        # Parse JSON response
        try:
            # Handle potential markdown code blocks
            text = text.strip()
            if text.startswith("```"):
                text = re.sub(r"^```\w*\n?", "", text)
                text = re.sub(r"\n?```$", "", text)
                text = text.strip()

            data = json.loads(text)
            primary = data.get("primary", "")
            secondary = data.get("secondary") or None

            if primary not in VALID_SUBFIELDS:
                log.warning(f"  Entry {review_id}: invalid primary '{primary}'")
                parse_errors += 1
                continue
            if secondary and secondary not in VALID_SUBFIELDS:
                secondary = None

            conn.execute(
                "UPDATE reviews SET subfield_primary = ?, subfield_secondary = ? WHERE id = ?",
                (primary, secondary, int(review_id)),
            )
            applied += 1

        except (json.JSONDecodeError, KeyError) as e:
            log.warning(f"  Entry {review_id}: parse error — {e} — text: {text[:100]}")
            parse_errors += 1

    conn.commit()
    conn.close()
    log.info(f"Applied {applied} classifications, {parse_errors} parse errors")
    return applied


# ---------------------------------------------------------------------------
# Single-entry classification (for weekly updates)
# ---------------------------------------------------------------------------
def classify_single(title, journal, abstract=None):
    """Classify a single entry using Haiku. Returns (primary, secondary)."""
    try:
        import anthropic
    except ImportError:
        return None, None

    msg = build_user_message(title, journal, abstract if abstract else None)

    client = anthropic.Anthropic()
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=60,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": msg}],
        )
        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```\w*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
            text = text.strip()

        data = json.loads(text)
        primary = data.get("primary", "")
        secondary = data.get("secondary") or None

        if primary not in VALID_SUBFIELDS:
            return None, None
        if secondary and secondary not in VALID_SUBFIELDS:
            secondary = None

        return primary, secondary
    except Exception as e:
        log.warning(f"Classification failed for '{title}': {e}")
        return None, None


def classify_new_reviews():
    """Classify newly added reviews that don't have subfields yet.
    Used by update.py after scrapers run."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row

    # First: apply journal map to new entries
    journal_classified = 0
    for journal, (primary, secondary) in JOURNAL_SUBFIELD_MAP.items():
        rows = conn.execute(
            "SELECT id FROM reviews "
            "WHERE publication_source = ? AND (subfield_primary IS NULL OR subfield_primary = '')",
            (journal,),
        ).fetchall()
        for row in rows:
            conn.execute(
                "UPDATE reviews SET subfield_primary = ?, subfield_secondary = ? WHERE id = ?",
                (primary, secondary, row["id"]),
            )
            journal_classified += 1

    conn.commit()

    # Second: use LLM for remaining unclassified entries
    rows = conn.execute(
        "SELECT id, book_title, publication_source, review_summary "
        "FROM reviews WHERE subfield_primary IS NULL OR subfield_primary = '' "
        "ORDER BY id DESC LIMIT 200"
    ).fetchall()

    llm_classified = 0
    for row in rows:
        title = row["book_title"] or ""
        journal = row["publication_source"] or ""
        abstract = row["review_summary"] or ""

        if not title:
            continue

        primary, secondary = classify_single(title, journal, abstract if abstract else None)
        if primary:
            conn.execute(
                "UPDATE reviews SET subfield_primary = ?, subfield_secondary = ? WHERE id = ?",
                (primary, secondary, row["id"]),
            )
            llm_classified += 1

        # Small delay to avoid rate limits
        time.sleep(0.1)

    conn.commit()
    conn.close()

    log.info(f"New review classification: {journal_classified} by journal map, "
             f"{llm_classified} by LLM")
    return journal_classified + llm_classified


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
def show_stats():
    """Show classification statistics."""
    conn = get_conn()

    total = conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
    classified = conn.execute(
        "SELECT COUNT(*) FROM reviews WHERE subfield_primary IS NOT NULL AND subfield_primary != ''"
    ).fetchone()[0]
    unclassified = total - classified

    print(f"\nTotal entries: {total:,}")
    print(f"Classified:   {classified:,} ({classified * 100 / total:.1f}%)")
    print(f"Unclassified: {unclassified:,} ({unclassified * 100 / total:.1f}%)")

    print("\nBy subfield:")
    rows = conn.execute(
        "SELECT subfield_primary, COUNT(*) as cnt FROM reviews "
        "WHERE subfield_primary IS NOT NULL AND subfield_primary != '' "
        "GROUP BY subfield_primary ORDER BY cnt DESC"
    ).fetchall()
    for code, cnt in rows:
        name = SUBFIELD_DISPLAY.get(code, code)
        print(f"  {name:45s} {cnt:>6,}")

    print("\nTop 20 unclassified journals:")
    rows = conn.execute(
        "SELECT publication_source, COUNT(*) as cnt FROM reviews "
        "WHERE subfield_primary IS NULL OR subfield_primary = '' "
        "GROUP BY publication_source ORDER BY cnt DESC LIMIT 20"
    ).fetchall()
    for journal, cnt in rows:
        print(f"  {journal:50s} {cnt:>6,}")

    conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="PhilReviews subfield classification")
    parser.add_argument("--journal-map-only", action="store_true",
                        help="Apply journal-based classification only")
    parser.add_argument("--fetch-abstracts", action="store_true",
                        help="Fetch Crossref abstracts for entries with DOIs")
    parser.add_argument("--batch", action="store_true",
                        help="Submit batch API classification")
    parser.add_argument("--batch-limit", type=int, default=None,
                        help="Limit batch to N entries (for testing)")
    parser.add_argument("--check-batch", type=str, metavar="BATCH_ID",
                        help="Check batch status and apply results")
    parser.add_argument("--stats", action="store_true",
                        help="Show classification statistics")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't write to database")
    args = parser.parse_args()

    if args.stats:
        show_stats()
        return

    if args.journal_map_only:
        n = apply_journal_map(dry_run=args.dry_run)
        log.info(f"Done. Classified {n} entries by journal map.")
        show_stats()
        return

    if args.fetch_abstracts:
        n = fetch_crossref_abstracts()
        log.info(f"Done. Fetched {n} abstracts.")
        return

    if args.batch:
        batch_ids = submit_batch(limit=args.batch_limit)
        if batch_ids:
            print(f"\nBatch IDs: {', '.join(batch_ids)}")
            print("Check status with: python3 classify_subfields.py --check-batch <BATCH_ID>")
        return

    if args.check_batch:
        check_and_apply_batch(args.check_batch)
        show_stats()
        return

    # Default: run full pipeline
    parser.print_help()


if __name__ == "__main__":
    main()
