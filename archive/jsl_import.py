#!/usr/bin/env python3
"""
One-time import of Journal of Symbolic Logic book reviews from Crossref.

JSL published book reviews from 1936-1999. All items are typed 'journal-article'
on Crossref, with the full bibliographic citation embedded in the title field.
This script fetches all JSL items, filters to book reviews (excluding article
reviews and omnibus reviews), parses the citations, and inserts into the DB.

Usage:
    python3 jsl_import.py                # full import
    python3 jsl_import.py --dry-run      # preview without inserting
    python3 jsl_import.py --fetch-only   # just download and cache Crossref data
    python3 jsl_import.py --sample 20    # show 20 parsed samples, don't insert
"""

import argparse
import html
import json
import os
import re
import sys
import time

import requests

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
import db

CACHE_FILE = os.path.join(ROOT, "jsl_crossref_cache.json")

# Publisher keywords that distinguish book reviews from article reviews
PUBLISHER_KW = re.compile(
    r'\b(?:Press|University|Books|Publishing|Verlag|Routledge|Springer|Polity|'
    r'Palgrave|Bloomsbury|Clarendon|Academic|Reidel|North-Holland|'
    r'Elsevier|Wiley|Harper|Macmillan|Blackwell|Methuen|Allen\s*&\s*Unwin|'
    r'Pergamon|Humanities\s+Press|Van\s+Nostrand|Dover|Freeman|Nijhoff|'
    r'Herder|Gauthier-Villars|Teubner|Louvain|Nauwelaerts|Meiner|'
    r'Duncker|Barth|Mohr|Presses\s+Universitaires|Éditions|Edizioni|'
    r'Libraire|Librairie|Wolters|Noordhoff|Munksgaard|Almqvist|'
    r'North Holland|D\.\s*Reidel|Kluwer|Plenum|Birkhäuser|'
    r'Addison-Wesley|Benjamin|Cummings|Holt|Saunders|'
    r'Van\s+Gorcum|Heinemann|Faber|Kegan\s+Paul|'
    r'Duckworth|Hodder|Penguin|Putnam|Random\s+House|'
    r'Scribner|Viking|Knopf|Simon|Doubleday)\b',
    re.IGNORECASE
)

# Series/edition keywords (not part of the book title)
SERIES_KW = re.compile(
    r'\bvol\.\s*\d|,\s*no\.\s*\d|\bseries\b|\bedition\b|\bLecture\s+notes\b|'
    r'\bStudies\s+in\s+logic\b|\bEncyclopedia\b|\bBibliotheca\b|\bTracts\b|'
    r'\bMonographs\b|\bHandbook\b|\bCollected\s+works\b',
    re.IGNORECASE
)

# Page count pattern (e.g., "viii + 372 pp.", "174 pp.", "pp. 1-234")
PAGE_PATTERN = re.compile(
    r'\b(?:[xivlc]+\s*\+\s*)?\d+\s+pp\b|\bpp\.\s*\d',
    re.IGNORECASE
)

# Article review pattern: journal citation with volume(year) and page range
ARTICLE_REVIEW_PATTERN = re.compile(
    r'vol\.\s*\d+\s*\(\d{4}\).*?pp\.\s*\d+[–\-]\d+',
    re.IGNORECASE
)


def fetch_all_jsl_items():
    """Fetch all JSL items from Crossref API with cursor pagination."""
    if os.path.exists(CACHE_FILE):
        print(f"Loading cached data from {CACHE_FILE}")
        with open(CACHE_FILE) as f:
            return json.load(f)

    print("Fetching all JSL items from Crossref...")
    all_items = []
    params = {
        "filter": "issn:0022-4812",
        "rows": 100,
        "cursor": "*",
        "mailto": "mzwolinski@sandiego.edu",
    }

    while True:
        resp = requests.get(
            "https://api.crossref.org/works",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("message", {}).get("items", [])
        if not batch:
            break

        all_items.extend(batch)
        total = data["message"]["total-results"]
        print(f"  Fetched {len(all_items)}/{total} items...")

        next_cursor = data.get("message", {}).get("next-cursor")
        if not next_cursor:
            break
        params["cursor"] = next_cursor
        time.sleep(0.5)

    print(f"Total items fetched: {len(all_items)}")

    # Cache for reprocessing
    with open(CACHE_FILE, "w") as f:
        json.dump(all_items, f)
    print(f"Cached to {CACHE_FILE}")

    return all_items


def is_book_review(item):
    """Determine if a Crossref item is a book review (not article review)."""
    title = item.get("title", [""])[0]

    # Skip omnibus reviews
    if "[Omnibus Review]" in title or title.strip() == "":
        return False

    # "Review:" prefix (newer 1991 format) — always a review
    if re.match(r'^Review:\s', title):
        return True

    # Exclude entries with journal citation patterns (vol. N (YYYY), pp. N-N)
    # even if they also mention a publisher (e.g., reprinted articles)
    if ARTICLE_REVIEW_PATTERN.search(title):
        return False

    # Must have a publisher keyword
    if not PUBLISHER_KW.search(title):
        return False

    # Must have a page count indicator OR be clearly a book citation
    if PAGE_PATTERN.search(title):
        return True

    # Some entries have publisher but no explicit "pp." — still book reviews
    # if they have a year pattern near the publisher
    if re.search(r'\b(?:19|20)\d{2}\b', title) and PUBLISHER_KW.search(title):
        return True

    return False


def is_article_review(title):
    """Check if a title is reviewing a journal article rather than a book."""
    # Article reviews cite: "Journal Name, vol. X (YYYY), pp. N-N"
    # But NOT if there's also a publisher keyword (could be a book in a series)
    if ARTICLE_REVIEW_PATTERN.search(title) and not PUBLISHER_KW.search(title):
        return True
    return False


def clean_title(text):
    """Clean up a parsed title string."""
    # Decode HTML entities
    text = html.unescape(text)
    # Strip HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Fix double periods from splitting
    text = re.sub(r'\.\.+', '.', text)
    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    # Strip trailing period
    text = text.rstrip('.')
    return text


def parse_review_title(title):
    """Parse a JSL review title to extract book author and book title.

    Returns dict with book_title, book_author_first, book_author_last,
    or None if parsing fails.
    """
    if not title:
        return None

    # Decode HTML entities first
    title = html.unescape(title)
    # Strip HTML tags for parsing
    title_clean = re.sub(r'<[^>]+>', '', title)
    # Strip "Reviews - " prefix
    title_clean = re.sub(r'^Reviews\s*[-–]\s*', '', title_clean)

    # Handle multi-item reviews: take only the first item
    # Multi-item delimiter is " - " (space-dash-space) between complete citations
    # Only split if what follows looks like a new author name (capitalized name pattern)
    parts = re.split(r'\s+[-–]\s+(?=[A-Z][a-z]+(?:\s+[A-Z])?)', title_clean, maxsplit=1)
    title_clean = parts[0].strip()

    # ── Format 1: "Review: Author(s), Book Title" ──
    m = re.match(r'^Review:\s*(.+)', title_clean)
    if m:
        result = parse_review_format(m.group(1).strip())
        if result:
            result["book_title"] = clean_title(result["book_title"])
        return result

    # ── Format 2a: "Title, edited by Editor, Publisher, Year, pp." (no periods) ──
    m = re.match(r'^(.+?),\s+edited\s+by\s+(.+?)(?:,\s+(?:' + PUBLISHER_KW.pattern + r'))', title_clean)
    if m:
        book_title = clean_title(m.group(1))
        # The editor is the "author" for an edited volume
        editor_str = m.group(2).strip().rstrip(",")
        first, last = split_name(editor_str)
        return {"book_title": book_title, "book_author_first": first, "book_author_last": last}

    # ── Format 2b: "Author. BookTitle. [Series.] Publisher, Location, Year, pp." ──
    result = parse_biblio_format(title_clean)
    if result:
        result["book_title"] = clean_title(result["book_title"])
    return result


def parse_review_format(text):
    """Parse 'Review: Author(s), Book Title' format."""
    # Split on ", " and figure out where author names end and title begins
    # Names look like "First [M.] Last" — short segments with initials
    # Titles are longer multi-word phrases
    segments = [s.strip() for s in text.split(", ")]

    if len(segments) < 2:
        return {"book_title": text, "book_author_first": "", "book_author_last": ""}

    # Find the split point: walk from the end, looking for where title starts
    # Title segments tend to be longer and not look like person names
    title_start = len(segments)
    for i in range(len(segments) - 1, 0, -1):
        seg = segments[i]
        # If this segment looks like a person name, the title starts after it
        if _looks_like_name(seg):
            title_start = i + 1
            break
        title_start = i

    # If we couldn't find a clear split, assume first segment = author, rest = title
    if title_start >= len(segments):
        title_start = 1

    author_parts = segments[:title_start]
    title_parts = segments[title_start:]

    author_str = ", ".join(author_parts)
    book_title = ", ".join(title_parts) if title_parts else text

    # If we ended up with no title (all segments look like names), take the last one
    if not book_title:
        book_title = segments[-1]
        author_str = ", ".join(segments[:-1])

    first, last = split_name(author_str)
    return {"book_title": book_title, "book_author_first": first, "book_author_last": last}


def _looks_like_name(s):
    """Check if a string looks like a person name."""
    words = s.split()
    if not words or len(words) > 5:
        return False
    # Names have mostly short capitalized words or initials
    name_like = 0
    for w in words:
        if re.match(r'^[A-Z]\.$', w):  # Initial
            name_like += 1
        elif re.match(r'^[A-Z][a-z]+$', w):  # Capitalized word
            name_like += 1
        elif re.match(r"^[A-Z][a-z]+['-][A-Z]?[a-z]*$", w):  # Hyphenated/apostrophe
            name_like += 1
        elif re.match(r'^(?:de|van|von|di|du|le|la|el|al|ibn)$', w, re.IGNORECASE):
            name_like += 1
    return name_like >= len(words) * 0.7 and len(words) <= 4


def parse_biblio_format(title):
    """Parse 'Author. BookTitle. [Series.] Publisher, Location, Year, pp.' format."""
    # Split on ". " (period-space) — but be careful with initials
    # Strategy: find the publisher keyword position, work backwards
    segments = split_on_period_space(title)

    if not segments:
        return None

    # Find the segment containing a publisher keyword
    pub_idx = None
    for i, seg in enumerate(segments):
        if PUBLISHER_KW.search(seg):
            pub_idx = i
            break

    if pub_idx is None:
        # No publisher found — might still be a valid entry
        # Try treating the whole thing as "Author. Title"
        if len(segments) >= 2:
            author_str = segments[0]
            book_title = ". ".join(segments[1:])
            # Clean trailing page info
            book_title = re.sub(r',?\s*(?:[xivlc]+\s*\+\s*)?\d+\s+pp\.?$', '', book_title)
            book_title = re.sub(r',?\s*\d{4}$', '', book_title)
            first, last = split_name(author_str)
            return {"book_title": book_title.strip(), "book_author_first": first, "book_author_last": last}
        return None

    # Segments before publisher
    before_pub = segments[:pub_idx]

    if not before_pub:
        return None

    # Strip series info segments from the end (working backwards)
    # Series info: "Studies in logic, vol. 100", "Synthese library", "Universitext", etc.
    title_idx = len(before_pub) - 1
    while title_idx > 0 and SERIES_KW.search(before_pub[title_idx]):
        title_idx -= 1

    if title_idx < 0:
        return None

    # Extract book title and author
    if title_idx == 0:
        # Only one segment before publisher — it's either just the title or author.title combined
        combined = before_pub[0]
        # Look for "AuthorName. BookTitle" pattern — author ends at period followed by title
        # Try to find the first ". " that separates the author from the book title
        # Author names: "J. H. Woodger", "Rudolf Carnap", "C. Smoryński"
        # Try splitting after what looks like a surname (word with 3+ lowercase chars)
        name_match = re.match(
            r'^((?:[A-Z]\.?\s+)*[A-Z][a-zA-Zéèüöäß\'-]+(?:\s+[A-Z][a-zA-Zéèüöäß\'-]+)*)\.\s+(.+)',
            combined
        )
        if name_match:
            candidate_author = name_match.group(1)
            candidate_title = name_match.group(2)
            # Verify the author part looks like a name (not too long)
            if len(candidate_author.split()) <= 6:
                author_str = candidate_author
                book_title = candidate_title
            else:
                return {"book_title": combined, "book_author_first": "", "book_author_last": ""}
        else:
            return {"book_title": combined, "book_author_first": "", "book_author_last": ""}
    else:
        # Multiple segments: first = author, rest up to title_idx = book title
        author_str = segments[0]
        book_title = ". ".join(before_pub[1:title_idx + 1])

    # Clean up book title
    book_title = book_title.strip().rstrip(".")

    first, last = split_name(author_str)
    return {"book_title": book_title, "book_author_first": first, "book_author_last": last}


def split_on_period_space(text):
    """Split text on '. ' boundaries, handling initials correctly.

    Initials like 'J. H.' have period-space but should not be split points.
    A real split point has period-space followed by a multi-char capitalized word.
    """
    result = []
    current = []
    i = 0
    chars = text

    while i < len(chars):
        if chars[i] == '.' and i + 1 < len(chars) and chars[i + 1] == ' ':
            # Check if this is an initial (single letter before the period)
            # and the next word is also short (another initial or name part)
            before = ''.join(current).strip()
            if before and len(before.split()[-1].rstrip('.')) <= 2:
                # Likely an initial — keep going
                current.append(chars[i])
                i += 1
                continue

            # Check what follows: if it's a single capital letter + period, it's an initial
            rest = chars[i + 2:i + 5] if i + 2 < len(chars) else ""
            if re.match(r'^[A-Z]\.\s', rest) or re.match(r'^[A-Z]\.$', rest):
                current.append(chars[i])
                i += 1
                continue

            # Real split point
            current.append('.')
            result.append(''.join(current).strip())
            current = []
            i += 2  # skip ". "
        else:
            current.append(chars[i])
            i += 1

    remaining = ''.join(current).strip()
    if remaining:
        result.append(remaining)

    return result


def split_name(name_str):
    """Split a name string into (first_name, last_name).

    Handles multiple authors by putting all in the combined fields.
    """
    if not name_str:
        return ("", "")

    name_str = name_str.strip().rstrip(".")

    # Multiple authors: "A and B" or "A, B, and C"
    # For display, keep them combined
    parts = name_str.split()
    if not parts:
        return ("", "")

    if len(parts) == 1:
        return ("", parts[0])

    return (" ".join(parts[:-1]), parts[-1])


def extract_date(item):
    """Extract publication date from Crossref item."""
    for field in ("published-print", "published-online", "issued"):
        date_info = item.get(field, {})
        date_parts = date_info.get("date-parts", [[]])
        if date_parts and date_parts[0]:
            parts = date_parts[0]
            if len(parts) >= 3:
                return f"{parts[0]}-{parts[1]:02d}-{parts[2]:02d}"
            elif len(parts) >= 1:
                return str(parts[0])
    return ""


def process_items(items):
    """Process Crossref items into review records."""
    records = []
    skipped_article = 0
    skipped_parse = 0
    skipped_omnibus = 0

    for item in items:
        title = item.get("title", [""])[0]

        if not is_book_review(item):
            if is_article_review(title):
                skipped_article += 1
            continue

        if "[Omnibus Review]" in title:
            skipped_omnibus += 1
            continue

        parsed = parse_review_title(title)
        if not parsed or not parsed.get("book_title"):
            skipped_parse += 1
            continue

        # Reviewer is the Crossref author
        authors = item.get("author", [])
        if authors:
            reviewer_first = authors[0].get("given", "")
            reviewer_last = authors[0].get("family", "")
        else:
            reviewer_first = ""
            reviewer_last = ""

        doi = item.get("DOI", "")
        date = extract_date(item)

        records.append({
            "book_title": parsed["book_title"],
            "book_author_first_name": parsed["book_author_first"],
            "book_author_last_name": parsed["book_author_last"],
            "reviewer_first_name": reviewer_first,
            "reviewer_last_name": reviewer_last,
            "publication_source": "The Journal of Symbolic Logic",
            "publication_date": date,
            "review_link": f"https://doi.org/{doi}" if doi else "",
            "review_summary": "",
            "access_type": "Restricted",
            "doi": doi,
            "entry_type": "review",
            "symposium_group": "",
            "_raw_title": title,  # Keep for debugging, won't be inserted
        })

    print(f"\nFiltering results:")
    print(f"  Book reviews found: {len(records)}")
    print(f"  Article reviews skipped: {skipped_article}")
    print(f"  Omnibus reviews skipped: {skipped_omnibus}")
    print(f"  Parse failures: {skipped_parse}")

    return records


def main():
    parser = argparse.ArgumentParser(description="Import JSL book reviews from Crossref")
    parser.add_argument("--dry-run", action="store_true", help="Preview without inserting")
    parser.add_argument("--fetch-only", action="store_true", help="Just download and cache data")
    parser.add_argument("--sample", type=int, help="Show N parsed samples")
    args = parser.parse_args()

    # Step 1: Fetch
    items = fetch_all_jsl_items()
    print(f"Total Crossref items: {len(items)}")

    if args.fetch_only:
        return

    # Step 2-3: Filter and parse
    records = process_items(items)

    # Show sample
    if args.sample or args.dry_run:
        n = args.sample or 20
        print(f"\n{'='*70}")
        print(f"Sample of {min(n, len(records))} parsed records:")
        print(f"{'='*70}")
        for r in records[:n]:
            print(f"  Book:     {r['book_title']}")
            print(f"  Author:   {r['book_author_first_name']} {r['book_author_last_name']}")
            print(f"  Reviewer: {r['reviewer_first_name']} {r['reviewer_last_name']}")
            print(f"  Date:     {r['publication_date']}")
            print(f"  Raw:      {r['_raw_title'][:100]}...")
            print()

    if args.dry_run or args.sample:
        print(f"Total records: {len(records)} (dry run, not inserted)")
        # Show author coverage
        with_author = sum(1 for r in records if r["book_author_last_name"])
        print(f"Author coverage: {with_author}/{len(records)} ({100*with_author/max(len(records),1):.0f}%)")
        return

    # Step 4: Insert
    # Remove debug field
    for r in records:
        r.pop("_raw_title", None)

    # Dedup against existing DB
    new_records = []
    dupes = 0
    for r in records:
        if r["doi"] and db.doi_exists(r["doi"]):
            dupes += 1
            continue
        if r["review_link"] and db.review_link_exists(r["review_link"]):
            dupes += 1
            continue
        new_records.append(r)

    print(f"\nDuplicates already in DB: {dupes}")
    print(f"New records to insert: {len(new_records)}")

    if new_records:
        db.insert_reviews(new_records)
        print(f"Inserted {len(new_records)} JSL book reviews")

        # Classify subfields
        try:
            from classify_subfields import classify_new_reviews
            classified = classify_new_reviews()
            print(f"Subfield classification: {classified} entries classified")
        except Exception as e:
            print(f"Subfield classification failed: {e}")
    else:
        print("No new records to insert")


if __name__ == "__main__":
    main()
