#!/usr/bin/env python3
"""
One-time data quality sweep:
1. Re-fetch raw Crossref titles for entries with missing authors
2. Re-parse with improved parser
3. Clean overly long titles (strip bibliographic metadata)
4. Update the database
"""

import re
import sys
import time
import sqlite3
import requests
from crossref_scraper import parse_review_title, _looks_like_author_name, _extract_first_author, is_book_review

DB_PATH = 'reviews.db'
SESSION = requests.Session()
SESSION.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'


def get_missing_author_entries():
    """Get all entries with missing book authors."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, doi, book_title, book_author_first_name, book_author_last_name, "
        "publication_source FROM reviews "
        "WHERE (book_author_last_name IS NULL OR book_author_last_name = '')"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def fetch_crossref_title(doi):
    """Fetch the raw title from Crossref for a given DOI."""
    try:
        resp = SESSION.get(f'https://api.crossref.org/works/{doi}', timeout=15)
        if resp.status_code == 200:
            data = resp.json()['message']
            raw_title = data.get('title', [''])[0]
            subtitle = data.get('subtitle', [''])[0] if data.get('subtitle') else ''
            return raw_title, subtitle, data
        return None, None, None
    except Exception as e:
        print(f'  Error fetching {doi}: {e}')
        return None, None, None


def is_garbled_author(first: str, last: str) -> bool:
    """Check if an author name looks like garbled metadata."""
    combined = f'{first} {last}'.strip()
    if not combined:
        return False
    # ISBN patterns (including old-style 0-xxxx format)
    if re.search(r'ISBN|978[-\d]|97[89]\d|0-\d{3,}', combined, re.IGNORECASE):
        return True
    # Publisher/institution names
    if re.search(r'\bPress\b|\bUniversity\b|\bPublisher|\bVerlag\b|\bEditions?\b|\bPresses\b', combined, re.IGNORECASE):
        return True
    # Page counts
    if re.search(r'\bpp\.|\bpages\b|\bPp\b', combined, re.IGNORECASE):
        return True
    # Prices
    if re.search(r'[\$£€]\d|dollars?|\bRs\.', combined, re.IGNORECASE):
        return True
    # Year patterns in author names (e.g. "2009)" or "2011),")
    if re.search(r'\b(19|20)\d{2}\)?[,.]?\s*$', last):
        return True
    if re.search(r'\b(19|20)\d{2}\)', combined):
        return True
    # Publication metadata keywords
    if re.search(r'\bHardcover\b|\bPaperback\b|\bHbk\b|\bPbk\b', combined, re.IGNORECASE):
        return True
    # Suspiciously long last name (>25 chars)
    if len(last) > 25:
        return True
    # Last name contains digits
    if last and re.search(r'\d', last):
        return True
    # First name contains colons or semicolons (publisher city patterns)
    if first and re.search(r'[;:]', first):
        return True
    # First name starts with "&amp" (HTML entity)
    if first and first.startswith('&amp'):
        return True
    # Last name is a common non-name word
    non_names = {'Approaches', 'Alternative', 'Blackwell', 'Thought', 'rapports',
                 'pages', 'Press', 'Club', 'Allegory', 'Hegel'}
    if last in non_names:
        return True
    return False


def clean_title(title):
    """Strip bibliographic metadata from a book title."""
    if not title:
        return title

    original = title

    # Strip HTML tags
    title = re.sub(r'<[^>]+>', '', title).strip()

    # Strip "By Author Name" suffixes (with credentials)
    title = re.sub(
        r'\.\s+By\s+[A-Z][a-zA-Z.\s,]+(?:M\.A\.|Ph\.D\.|D\.Phil\.|Fellow|Lecturer|Professor|Director).*$',
        '', title
    ).strip()

    # Strip publisher info after title: ". Publisher, City, Year"
    title = re.sub(r'\.\s+(?:Franciscan Institute|Cambridge University|Oxford University|Princeton University|Routledge|Macmillan|Blackwell|Springer|Penguin|Harvard|Yale|MIT|Clarendon|Wiley|Palgrave|Rowman|SUNY|Cornell|Stanford|Duke|Indiana University|University of \w+|Fordham|Continuum|Polity|Ashgate|Brill|Kluwer|Sage|Verso|Pluto|Allen & Unwin|Humanities|Wadsworth|Prentice|Heritage|Bellarmin).*$', '', title, flags=re.IGNORECASE).strip()

    # Strip parenthetical publisher info: "(Publisher, Year, Pages)" or "(City: Publisher, Year)"
    title = re.sub(r'\s*\([A-Z][a-z]+(?:\s[A-Z][a-z]+)*(?::|,)\s+\d{4}[^)]*\)\s*$', '', title).strip()
    # Standalone parenthetical: "(Oxford: Oxford University Press, 2019)"
    title = re.sub(r'\s*\([A-Z][a-z]+(?:\s[A-Za-z]+)*:\s+[A-Z].*?(?:Press|Publishing|Publishers).*?\)\s*$', '', title).strip()
    # Bare parenthetical at start: entire title is "(City: Publisher, Year)"
    if re.match(r'^\([A-Z].*(?:Press|Publishing).*\)$', title):
        return original  # Don't clean if the entire title would be erased

    # Strip trailing price: "$12.95" / "£44.50" / "€ 18,80"
    title = re.sub(r'\s*[\$£]\d+[.\d]*\s*$', '', title).strip()
    title = re.sub(r'\s*€\s*\d+[,.\d]*\s*$', '', title).strip()

    # Strip trailing ISBN: "(ISBN xxx)" or "ISBN xxx"
    title = re.sub(r'\s*\(?ISBN[:\s]?[0-9X-]+\)?\s*$', '', title, flags=re.IGNORECASE).strip()

    # Strip trailing page info: "Pp. xxx" / "xxx pages" / "xii + 472"
    title = re.sub(r'\.?\s*Pp\.?\s+[xivlc\d+\s]+\.?\s*$', '', title, flags=re.IGNORECASE).strip()
    title = re.sub(r'\s*,?\s*\d+\s+pages?\s*\.?\s*$', '', title, flags=re.IGNORECASE).strip()
    title = re.sub(r'\s*,?\s*[xivlc]+\s*\+?\s*\d+\s*(?:pp?\.?)?\s*$', '', title, flags=re.IGNORECASE).strip()

    # Strip trailing year after period: ". 1977"
    title = re.sub(r'\.\s+\d{4}\s*$', '', title).strip()

    # Strip trailing city/publisher fragments: ", New York" / ". London:"
    title = re.sub(r'[,.]?\s+(?:New York|London|Cambridge|Oxford|Princeton|Chicago|Boston|Berkeley|Dordrecht|Leiden|The Hague|Ithaca|Toronto|Paris|Amsterdam|Berlin|Bloomington|Indianapolis|Philadelphia|Pittsburgh|Notre Dame|Englewood Cliffs|West Lafayette|St\. Bonaventure|Albany|Minneapolis|Charlottesville|Edinburgh|München|Munich|Frankfurt|Montréal|Sherbrooke|Geneva|Genève|Cardiff|New Haven)[,:\s].*$', '', title, flags=re.IGNORECASE).strip()

    # Strip "Rs. 150 ($30)" style price info
    title = re.sub(r'\s*Rs\.\s*\d+.*$', '', title).strip()

    # Clean trailing punctuation
    title = title.rstrip('.,;: ')

    # Don't return empty or very short result
    if len(title) < 3:
        return original

    return title


def update_entry(entry_id, book_title=None, first=None, last=None):
    """Update a review entry by ID."""
    conn = sqlite3.connect(DB_PATH)
    if book_title is not None and first is not None:
        conn.execute(
            "UPDATE reviews SET book_title = ?, book_author_first_name = ?, "
            "book_author_last_name = ? WHERE id = ?",
            (book_title, first, last, entry_id)
        )
    elif book_title is not None:
        conn.execute(
            "UPDATE reviews SET book_title = ? WHERE id = ?",
            (book_title, entry_id)
        )
    elif first is not None:
        conn.execute(
            "UPDATE reviews SET book_author_first_name = ?, "
            "book_author_last_name = ? WHERE id = ?",
            (first, last, entry_id)
        )
    conn.commit()
    conn.close()


def fix_garbled_authors():
    """Fix entries where author fields contain garbled metadata."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Use SQL-based detection for reliability — catches ISBNs, publishers,
    # page counts, prices, and suspiciously long last names
    rows = conn.execute(
        "SELECT id, doi, book_title, book_author_first_name, book_author_last_name, "
        "publication_source FROM reviews "
        "WHERE book_author_last_name != '' AND book_author_last_name IS NOT NULL "
        "AND (book_author_last_name LIKE '%ISBN%' "
        "  OR book_author_last_name LIKE '%Press%' "
        "  OR book_author_last_name LIKE '%University%' "
        "  OR book_author_last_name LIKE '%Publisher%' "
        "  OR book_author_last_name LIKE '%Verlag%' "
        "  OR book_author_last_name LIKE '%pp%' "
        "  OR book_author_last_name LIKE '%pages%' "
        "  OR book_author_last_name LIKE '%978-%' "
        "  OR book_author_last_name LIKE '%0-%' "
        "  OR book_author_last_name LIKE '%Hardcover%' "
        "  OR book_author_last_name LIKE '%Paperback%' "
        "  OR LENGTH(book_author_last_name) > 30)"
    ).fetchall()
    conn.close()

    garbled = [dict(r) for r in rows]
    print(f'Found {len(garbled)} entries with garbled author names')

    fixed = 0
    cleared = 0
    failed = 0

    with_doi = [e for e in garbled if e['doi']]
    without_doi = [e for e in garbled if not e['doi']]
    print(f'  With DOI (can re-fetch): {len(with_doi)}')
    print(f'  Without DOI: {len(without_doi)}')

    # Try re-fetching entries with DOIs
    for i, entry in enumerate(with_doi):
        raw_title, subtitle, crossref_data = fetch_crossref_title(entry['doi'])
        if raw_title:
            result = parse_review_title(raw_title, subtitle or '', crossref_data)
            if result and result.get('book_author_last') and not is_garbled_author(
                    result.get('book_author_first', ''), result['book_author_last']):
                clean_book_title = clean_title(result['book_title'])
                update_entry(entry['id'], book_title=clean_book_title,
                             first=result['book_author_first'], last=result['book_author_last'])
                fixed += 1
            else:
                # Re-parse failed — clear the garbled author
                update_entry(entry['id'], first='', last='')
                cleared += 1
        else:
            update_entry(entry['id'], first='', last='')
            cleared += 1

        if (i + 1) % 10 == 0:
            time.sleep(1)
        if (i + 1) % 100 == 0:
            print(f'  Processed {i + 1}/{len(with_doi)}: {fixed} fixed, {cleared} cleared')

    # Clear garbled authors without DOIs
    for entry in without_doi:
        update_entry(entry['id'], first='', last='')
        cleared += 1

    print(f'\nGarbled author fix results:')
    print(f'  Fixed with correct author: {fixed}')
    print(f'  Cleared (now empty): {cleared}')
    return fixed, cleared


def fix_missing_authors():
    """Re-fetch and re-parse entries with missing authors."""
    entries = get_missing_author_entries()
    print(f'Found {len(entries)} entries with missing authors')

    with_doi = [e for e in entries if e['doi']]
    without_doi = [e for e in entries if not e['doi']]
    print(f'  With DOI (can re-fetch): {len(with_doi)}')
    print(f'  Without DOI (cannot re-fetch): {len(without_doi)}')

    fixed = 0
    failed = 0
    by_journal = {}

    for i, entry in enumerate(with_doi):
        raw_title, subtitle, crossref_data = fetch_crossref_title(entry['doi'])
        if not raw_title:
            failed += 1
            continue

        result = parse_review_title(raw_title, subtitle or '', crossref_data)
        if result and result.get('book_author_last'):
            # Clean the title too
            clean_book_title = clean_title(result['book_title'])
            update_entry(
                entry['id'],
                book_title=clean_book_title,
                first=result['book_author_first'],
                last=result['book_author_last'],
            )
            fixed += 1
            journal = entry['publication_source']
            by_journal[journal] = by_journal.get(journal, 0) + 1
        else:
            failed += 1

        # Rate limiting: ~10/sec
        if (i + 1) % 10 == 0:
            time.sleep(1)

        if (i + 1) % 100 == 0:
            print(f'  Processed {i + 1}/{len(with_doi)}: {fixed} fixed, {failed} failed')

    print(f'\nAuthor fix results:')
    print(f'  Fixed: {fixed}')
    print(f'  Still missing: {failed + len(without_doi)}')
    print(f'  By journal:')
    for journal, count in sorted(by_journal.items(), key=lambda x: -x[1]):
        print(f'    {journal}: {count}')

    return fixed


def fix_long_titles():
    """Clean bibliographic metadata from overly long titles."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, book_title, publication_source FROM reviews "
        "WHERE LENGTH(book_title) > 150"
    ).fetchall()
    conn.close()

    print(f'\nFound {len(rows)} titles longer than 150 chars')

    fixed = 0
    for row in rows:
        row = dict(row)
        cleaned = clean_title(row['book_title'])
        if cleaned != row['book_title'] and len(cleaned) < len(row['book_title']):
            print(f'  [{row["publication_source"]}]')
            print(f'    Before: {row["book_title"][:100]}...')
            print(f'    After:  {cleaned[:100]}')
            update_entry(row['id'], book_title=cleaned)
            fixed += 1

    print(f'Cleaned {fixed} long titles')
    return fixed


def fix_all_titles_with_metadata():
    """Clean bibliographic metadata from ALL titles, not just long ones."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Look for titles with common bibliographic patterns
    rows = conn.execute(
        "SELECT id, book_title FROM reviews "
        "WHERE book_title LIKE '%Pp.%' "
        "   OR book_title LIKE '%pages%' "
        "   OR book_title LIKE '%ISBN%' "
        "   OR book_title LIKE '%$%' "
        "   OR book_title LIKE '%£%' "
        "   OR book_title LIKE '%. By %M.A.%' "
        "   OR book_title LIKE '%. By %Ph.D.%'"
    ).fetchall()
    conn.close()

    print(f'\nFound {len(rows)} titles with potential bibliographic metadata')

    fixed = 0
    for row in rows:
        row = dict(row)
        cleaned = clean_title(row['book_title'])
        if cleaned != row['book_title']:
            update_entry(row['id'], book_title=cleaned)
            fixed += 1

    print(f'Cleaned {fixed} titles with bibliographic metadata')
    return fixed


def remove_false_positive_reviews():
    """Remove research articles that were misidentified as book reviews.

    Detection: entries where the 'author' is actually a title fragment,
    created when parse_review_title() split a research article title at
    a comma, colon, or hyphen.

    For each suspect entry, re-fetch from Crossref and re-check with
    strict is_book_review(detection_mode='italic_only'). If it no longer
    qualifies as a book review, delete it. If it IS a legitimate review,
    re-parse to fix the author.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Find suspect entries: empty first name + non-empty last name (title fragment as "author")
    # OR first name contains articles/prepositions (another title-fragment pattern)
    rows = conn.execute("""
        SELECT id, doi, book_title, book_author_first_name, book_author_last_name,
               publication_source, review_link
        FROM reviews
        WHERE doi != ''
        AND (
            -- Pattern 1: empty first name + non-empty last name
            (book_author_first_name = '' AND book_author_last_name != ''
             AND LENGTH(book_author_last_name) > 0)
            OR
            -- Pattern 2: first name contains prepositions/articles (title fragments)
            (book_author_first_name LIKE '% of %' OR book_author_first_name LIKE '% and %'
             OR book_author_first_name LIKE '% the %' OR book_author_first_name LIKE '% in %'
             OR book_author_first_name LIKE '% for %' OR book_author_first_name LIKE '% to %'
             OR book_author_first_name LIKE '% a %' OR book_author_first_name LIKE '% an %'
             OR book_author_first_name LIKE '% on %' OR book_author_first_name LIKE '% with %'
             OR book_author_first_name LIKE '% from %' OR book_author_first_name LIKE '% or %'
             OR book_author_first_name LIKE '% is %' OR book_author_first_name LIKE '% as %')
            OR
            -- Pattern 3: "last name" is a multi-word phrase (clearly not a surname)
            (book_author_last_name LIKE '% % %')
        )
    """).fetchall()
    conn.close()

    suspects = [dict(r) for r in rows]
    print(f'Found {len(suspects)} suspect entries (potential false positives)')

    deleted = 0
    reparsed = 0
    kept = 0
    errors = 0
    by_journal_deleted = {}

    for i, entry in enumerate(suspects):
        doi = entry['doi']
        try:
            resp = SESSION.get(f'https://api.crossref.org/works/{doi}', timeout=15)
            if resp.status_code != 200:
                errors += 1
                continue

            crossref_data = resp.json()['message']

            # Re-check with strict mode (no name-based heuristics)
            if is_book_review(crossref_data, detection_mode='italic_only'):
                # Legitimate book review — re-parse to fix the author
                raw_title = (crossref_data.get('title', ['']) or [''])[0]
                subtitle = (crossref_data.get('subtitle', ['']) or [''])[0] if crossref_data.get('subtitle') else ''
                result = parse_review_title(raw_title, subtitle, crossref_data)
                if result and result.get('book_title'):
                    clean_book = clean_title(result['book_title'])
                    first = result.get('book_author_first', '')
                    last = result.get('book_author_last', '')
                    update_entry(entry['id'], book_title=clean_book, first=first, last=last)
                    reparsed += 1
                else:
                    kept += 1
            else:
                # NOT a book review — delete it
                conn = sqlite3.connect(DB_PATH)
                conn.execute("DELETE FROM reviews WHERE id = ?", (entry['id'],))
                conn.commit()
                conn.close()
                deleted += 1
                journal = entry['publication_source']
                by_journal_deleted[journal] = by_journal_deleted.get(journal, 0) + 1

        except Exception as e:
            errors += 1

        # Rate limiting
        if (i + 1) % 10 == 0:
            time.sleep(1)

        if (i + 1) % 200 == 0:
            print(f'  Processed {i + 1}/{len(suspects)}: {deleted} deleted, {reparsed} re-parsed, {errors} errors')

    print(f'\nFalse positive removal results:')
    print(f'  Deleted (not book reviews): {deleted}')
    print(f'  Re-parsed (fixed author): {reparsed}')
    print(f'  Kept as-is: {kept}')
    print(f'  Errors: {errors}')
    if by_journal_deleted:
        print(f'  Deletions by journal:')
        for journal, count in sorted(by_journal_deleted.items(), key=lambda x: -x[1]):
            print(f'    {journal}: {count}')

    return deleted, reparsed


def main():
    print('=== PhilReviews Data Quality Sweep Round 2 ===\n')

    # Check baseline
    conn = sqlite3.connect(DB_PATH)
    missing_before = conn.execute(
        "SELECT COUNT(*) FROM reviews WHERE book_author_last_name IS NULL OR book_author_last_name = ''"
    ).fetchone()[0]
    long_before = conn.execute(
        "SELECT COUNT(*) FROM reviews WHERE LENGTH(book_title) > 200"
    ).fetchone()[0]
    garbled_before = conn.execute(
        "SELECT COUNT(*) FROM reviews WHERE book_author_last_name != '' "
        "AND book_author_last_name IS NOT NULL"
    ).fetchone()[0]  # Will count in fix_garbled_authors
    conn.close()
    print(f'Baseline: {missing_before} missing authors, {long_before} titles >200 chars\n')

    # Step 1: Fix garbled authors (ISBNs, publishers in author fields)
    print('--- Step 1: Fix garbled author names ---')
    fix_garbled_authors()

    # Step 2: Fix missing authors via Crossref re-fetch
    print('\n--- Step 2: Fix missing authors via Crossref re-fetch ---')
    fix_missing_authors()

    # Step 3: Clean long titles
    print('\n--- Step 3: Clean long titles ---')
    fix_long_titles()

    # Step 4: Clean bibliographic metadata from all titles
    print('\n--- Step 4: Clean bibliographic metadata from all titles ---')
    fix_all_titles_with_metadata()

    # Final check
    conn = sqlite3.connect(DB_PATH)
    missing_after = conn.execute(
        "SELECT COUNT(*) FROM reviews WHERE book_author_last_name IS NULL OR book_author_last_name = ''"
    ).fetchone()[0]
    long_after = conn.execute(
        "SELECT COUNT(*) FROM reviews WHERE LENGTH(book_title) > 200"
    ).fetchone()[0]
    conn.close()

    print(f'\n=== Summary ===')
    print(f'Missing authors: {missing_before} → {missing_after} ({"+" if missing_after > missing_before else ""}{missing_after - missing_before})')
    print(f'Long titles (>200): {long_before} → {long_after} (fixed {long_before - long_after})')


if __name__ == '__main__':
    main()
