#!/usr/bin/env python3
"""
Data cleanup: presentation fixes for PhilReviews database.
Fixes: ALL CAPS titles, leading punctuation, HTML entities/tags,
generic "Book review" entries, (ed.) in author names, first/last name swaps.
"""

import html
import re
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reviews.db")

# Words to keep lowercase in title case (unless first word or after colon)
LOWERCASE_WORDS = {
    'a', 'an', 'the', 'and', 'but', 'or', 'nor', 'for', 'yet', 'so',
    'in', 'on', 'at', 'to', 'by', 'of', 'up', 'as', 'is', 'if',
    'it', 'be', 'do', 'no', 'vs', 'via', 'de', 'du', 'et', 'la',
    'le', 'les', 'des', 'en', 'un', 'une', 'von', 'van', 'der',
    'das', 'dem', 'den', 'die', 'und', 'aus', 'mit',
}

# Common acronyms/abbreviations to preserve as uppercase
ACRONYMS = {
    'II', 'III', 'IV', 'VI', 'VII', 'VIII', 'IX', 'XI', 'XII',
    'XIII', 'XIV', 'XV', 'XVI', 'XVII', 'XVIII', 'XIX', 'XX', 'XXI',
    'USA', 'UK', 'EU', 'UN', 'NATO', 'USSR', 'DNA', 'RNA', 'AIDS',
    'HIV', 'IQ', 'GDP', 'CIA', 'FBI', 'MIT', 'UCLA', 'NYU',
}


def smart_title_case(s: str) -> str:
    """Convert ALL CAPS string to title case, preserving acronyms."""
    words = s.split()
    result = []
    after_colon = False
    for i, word in enumerate(words):
        upper = word.upper().rstrip('.,;:!?')
        # Check if previous word ended with colon
        if i > 0 and result[-1].endswith(':'):
            after_colon = True
        else:
            after_colon = False

        # Preserve Roman numerals and known acronyms
        if upper in ACRONYMS:
            result.append(word.upper() if word == word.upper() else word)
        # First word or after colon: always capitalize
        elif i == 0 or after_colon:
            result.append(word.capitalize())
        # Lowercase articles/prepositions
        elif word.lower() in LOWERCASE_WORDS:
            result.append(word.lower())
        else:
            result.append(word.capitalize())
    return ' '.join(result)


def fix_all_caps_titles(conn):
    """Convert ALL CAPS titles to title case (ASCII-only, skip Hebrew etc.)."""
    cursor = conn.execute("""
        SELECT id, book_title FROM reviews
        WHERE book_title = UPPER(book_title)
        AND LENGTH(book_title) > 10
    """)
    rows = cursor.fetchall()

    fixed = 0
    for row_id, title in rows:
        # Only fix if it has uppercase ASCII letters and no lowercase ASCII
        has_upper = any(c.isupper() for c in title)
        has_lower = any(c.islower() for c in title)
        if not has_upper or has_lower:
            continue

        new_title = smart_title_case(title)
        if new_title != title:
            conn.execute("UPDATE reviews SET book_title = ? WHERE id = ?",
                         (new_title, row_id))
            fixed += 1
            if fixed <= 10:
                print(f"  {title[:60]}")
                print(f"    -> {new_title[:60]}")

    if fixed > 10:
        print(f"  ... and {fixed - 10} more")
    print(f"  Total: {fixed} ALL CAPS titles fixed")
    return fixed


def fix_leading_punctuation(conn):
    """Strip leading commas and periods (but not ellipsis)."""
    fixed = 0

    # Leading comma
    cursor = conn.execute("SELECT id, book_title FROM reviews WHERE book_title LIKE ',%'")
    for row_id, title in cursor.fetchall():
        new_title = title.lstrip(', ')
        if new_title != title:
            conn.execute("UPDATE reviews SET book_title = ? WHERE id = ?",
                         (new_title, row_id))
            fixed += 1
            print(f"  comma: '{title[:50]}' -> '{new_title[:50]}'")

    # Leading period (but NOT ellipsis like "...But Don't Overdo It" or ". . .")
    cursor = conn.execute("SELECT id, book_title FROM reviews WHERE book_title LIKE '.%'")
    for row_id, title in cursor.fetchall():
        if title.startswith('...') or title.startswith('. . .'):
            continue
        new_title = title.lstrip('. ')
        if new_title != title:
            conn.execute("UPDATE reviews SET book_title = ? WHERE id = ?",
                         (new_title, row_id))
            fixed += 1
            print(f"  period: '{title[:50]}' -> '{new_title[:50]}'")

    print(f"  Total: {fixed} leading punctuation entries fixed")
    return fixed


def fix_html_entities(conn):
    """Decode HTML entities like &amp; -> &."""
    cursor = conn.execute("""
        SELECT id, book_title FROM reviews
        WHERE book_title LIKE '%&amp;%'
        OR book_title LIKE '%&lt;%'
        OR book_title LIKE '%&gt;%'
        OR book_title LIKE '%&quot;%'
        OR book_title LIKE '%&#%'
        OR book_title LIKE '%&apos;%'
    """)
    rows = cursor.fetchall()
    fixed = 0
    for row_id, title in rows:
        new_title = html.unescape(title)
        if new_title != title:
            conn.execute("UPDATE reviews SET book_title = ? WHERE id = ?",
                         (new_title, row_id))
            fixed += 1
            if fixed <= 5:
                print(f"  '{title[:60]}' -> '{new_title[:60]}'")

    if fixed > 5:
        print(f"  ... and {fixed - 5} more")
    print(f"  Total: {fixed} HTML entity entries fixed")
    return fixed


def fix_html_tags(conn):
    """Strip known HTML tags (sup, scp, i, b, sub, em, strong) from titles."""
    # Only match known HTML tags, not angle-bracket titles like <Deutscher Empirismus>
    KNOWN_TAGS = r'(?:sup|scp|sub|i|b|em|strong)'
    cursor = conn.execute("""
        SELECT id, book_title FROM reviews
        WHERE book_title LIKE '%<%' AND book_title LIKE '%>%'
    """)
    rows = cursor.fetchall()
    fixed = 0
    for row_id, title in rows:
        # Only process if it contains known HTML tags
        if not re.search(rf'</?{KNOWN_TAGS}[^>]*>', title, re.IGNORECASE):
            continue
        # Replace <sup>ordinal</sup> with text, adding space before next word if needed
        # e.g., "4<sup>th</sup>Century" -> "4th Century"
        new_title = re.sub(r'<sup>(st|nd|rd|th)</sup>(?=[A-Z])', r'\1 ', title, flags=re.IGNORECASE)
        new_title = re.sub(r'<sup>(st|nd|rd|th)</sup>', r'\1', new_title, flags=re.IGNORECASE)
        # Remove <sup>N</sup> footnote markers entirely
        new_title = re.sub(r'<sup>\d+</sup>', '', new_title)
        # Replace <sup>text</sup> with text for remaining cases (e.g., 2<sup>nd</sup> -> 2nd)
        new_title = re.sub(r'<sup>([^<]+)</sup>', r'\1', new_title)
        # Unwrap other known tags: <scp>Text</scp> -> Text, <i>Text</i> -> Text
        new_title = re.sub(rf'<{KNOWN_TAGS}[^>]*>(.*?)</{KNOWN_TAGS}>', r'\1',
                           new_title, flags=re.IGNORECASE | re.DOTALL)
        # Strip any remaining known opening/closing tags
        new_title = re.sub(rf'</?{KNOWN_TAGS}[^>]*>', '', new_title, flags=re.IGNORECASE)
        new_title = new_title.strip()
        if new_title != title:
            conn.execute("UPDATE reviews SET book_title = ? WHERE id = ?",
                         (new_title, row_id))
            fixed += 1
            print(f"  '{title[:60]}' -> '{new_title[:60]}'")

    print(f"  Total: {fixed} HTML tag entries fixed")
    return fixed


def delete_generic_book_reviews(conn):
    """Delete entries with no real title, just 'Book review(s)'."""
    cursor = conn.execute("""
        SELECT id, book_title, publication_source, publication_date FROM reviews
        WHERE LOWER(book_title) IN ('book reviews', 'book review')
    """)
    rows = cursor.fetchall()
    count = len(rows)

    if count > 0:
        for row_id, title, source, date in rows[:5]:
            print(f"  Deleting: '{title}' ({source}, {date})")
        if count > 5:
            print(f"  ... and {count - 5} more")

        conn.execute("DELETE FROM reviews WHERE LOWER(book_title) IN ('book reviews', 'book review')")

    print(f"  Total: {count} generic 'Book review(s)' entries deleted")
    return count


def fix_editor_annotations(conn):
    """Fix entries where (ed.) / (eds.) leaked into book_author_last_name."""
    cursor = conn.execute("""
        SELECT id, book_title, book_author_first_name, book_author_last_name
        FROM reviews WHERE book_author_last_name LIKE '%(ed%'
    """)
    rows = cursor.fetchall()
    fixed = 0
    for row_id, title, first, last in rows:
        new_first = first or ''
        new_last = last or ''

        if last in ('(ed', '(Eds', '(eds'):
            # (ed was captured as last name; actual last name is last word of first_name
            if first:
                # Handle concatenated names like "RichardMenary"
                if re.match(r'^[A-Z][a-z]+[A-Z][a-z]+$', first):
                    # Split at camelCase boundary
                    m = re.match(r'^([A-Z][a-z]+)([A-Z][a-z]+)$', first)
                    if m:
                        new_first = m.group(1)
                        new_last = m.group(2)
                else:
                    parts = first.strip().split()
                    if len(parts) >= 2:
                        new_last = parts[-1]
                        new_first = ' '.join(parts[:-1])
                    elif len(parts) == 1:
                        new_last = parts[0]
                        new_first = ''
        elif '(ed' in last:
            # Strip (ed.), (eds.) suffix
            cleaned = re.sub(r'\s*\((?:ed|eds|Ed|Eds)\.?\)\s*$', '', last, flags=re.IGNORECASE).strip()
            if cleaned:
                if ' ' in cleaned:
                    # e.g. "Hilary Gatti (ed.)" — split into first/last
                    parts = cleaned.split()
                    new_last = parts[-1]
                    if not new_first:
                        new_first = ' '.join(parts[:-1])
                    else:
                        new_last = cleaned  # Keep as-is if first already populated
                else:
                    new_last = cleaned
            elif first:
                # last_name was purely "(eds.)" — derive last name from first_name
                parts = first.strip().split()
                if len(parts) >= 2:
                    new_last = parts[-1]
                    new_first = ' '.join(parts[:-1])
                elif len(parts) == 1:
                    new_last = parts[0]
                    new_first = ''

        if new_first != (first or '') or new_last != (last or ''):
            conn.execute(
                "UPDATE reviews SET book_author_first_name = ?, book_author_last_name = ? WHERE id = ?",
                (new_first, new_last, row_id))
            fixed += 1
            print(f"  [{row_id}] '{first}' / '{last}' -> '{new_first}' / '{new_last}'")

    print(f"  Total: {fixed} (ed.) entries fixed")
    return fixed


def fix_name_swaps(conn):
    """Swap first/last names where initials ended up in last_name."""
    cursor = conn.execute("""
        SELECT id, book_title, book_author_first_name, book_author_last_name, publication_source
        FROM reviews
        WHERE LENGTH(book_author_last_name) <= 3
        AND book_author_last_name LIKE '%.%'
        AND book_author_first_name IS NOT NULL
        AND LENGTH(book_author_first_name) > 3
    """)
    # Suffixes that look like initials but aren't
    SKIP_SUFFIXES = {'Jr.', 'Sr.', 'Jr', 'Sr',
                     'O.P', 'S.J.', 'O.S.B.', 'O.F.M.', 'C.S.C.', 'S.J',
                     'O.P.', 'O.S.B', 'O.F.M', 'C.S.C',
                     'al.', 'al',  # "et al." fragments
                     'M.D', 'Ph.D', 'D.D', 'M.A',  # academic degrees
                     'M.D.', 'Ph.D.', 'D.D.', 'M.A.'}

    rows = cursor.fetchall()
    fixed = 0
    skipped = 0
    for row_id, title, first, last, source in rows:
        # Sanity check: first_name should start with uppercase letter
        if not first or not first[0].isupper():
            skipped += 1
            continue

        # Don't swap if first_name also looks like initials
        if all(len(p) <= 3 for p in first.split()):
            skipped += 1
            continue

        # Don't swap Jr./Sr. suffixes or religious order abbreviations
        if last.rstrip('.') in {s.rstrip('.') for s in SKIP_SUFFIXES}:
            skipped += 1
            continue

        new_first = last   # the initials become first name
        new_last = first   # the surname becomes last name

        # Handle "Jarrett, James" format (Last, First) in the first_name field
        if ',' in new_last:
            parts = [p.strip() for p in new_last.split(',')]
            surname = parts[0]        # "Jarrett" or "McGrade"
            given = parts[1] if len(parts) > 1 else ''  # "James" or "Arthur"
            new_last = surname
            # Put given name before initial: "Arthur" + "S." → "Arthur S."
            new_first = (given + ' ' + new_first).strip() if given else new_first

        conn.execute(
            "UPDATE reviews SET book_author_first_name = ?, book_author_last_name = ? WHERE id = ?",
            (new_first, new_last, row_id))
        fixed += 1
        if fixed <= 10:
            print(f"  [{source}] '{first}' / '{last}' -> '{new_first}' / '{new_last}'")

    if fixed > 10:
        print(f"  ... and {fixed - 10} more")
    print(f"  Total: {fixed} name swaps fixed (skipped {skipped})")
    return fixed


def main():
    import argparse
    parser = argparse.ArgumentParser(description='PhilReviews presentation cleanup')
    parser.add_argument('--dry-run', action='store_true', help='Show changes without applying')
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    total_before = conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
    print(f"Database: {total_before} entries\n")

    print("1. ALL CAPS titles -> title case")
    caps_fixed = fix_all_caps_titles(conn)

    print("\n2. Leading punctuation")
    punct_fixed = fix_leading_punctuation(conn)

    print("\n3. HTML entities")
    entity_fixed = fix_html_entities(conn)

    print("\n4. HTML tags")
    tag_fixed = fix_html_tags(conn)

    print("\n5. Generic 'Book review' entries")
    deleted = delete_generic_book_reviews(conn)

    print("\n6. (ed.) in author last name")
    ed_fixed = fix_editor_annotations(conn)

    print("\n7. First/last name swaps")
    swap_fixed = fix_name_swaps(conn)

    # Summary
    total_after = conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
    total_fixes = caps_fixed + punct_fixed + entity_fixed + tag_fixed + ed_fixed + swap_fixed
    print(f"\n{'='*50}")
    print(f"SUMMARY")
    print(f"{'='*50}")
    print(f"ALL CAPS titles fixed:     {caps_fixed}")
    print(f"Leading punctuation fixed: {punct_fixed}")
    print(f"HTML entities fixed:       {entity_fixed}")
    print(f"HTML tags fixed:           {tag_fixed}")
    print(f"Generic entries deleted:   {deleted}")
    print(f"(ed.) annotations fixed:   {ed_fixed}")
    print(f"Name swaps fixed:          {swap_fixed}")
    print(f"Total fixes:               {total_fixes}")
    print(f"Total deletions:           {deleted}")
    print(f"Entries: {total_before} -> {total_after}")

    if args.dry_run:
        print("\nDry run -- rolling back all changes")
        conn.rollback()
    else:
        conn.commit()
        print("\nAll changes committed to database")

    conn.close()


if __name__ == '__main__':
    main()
