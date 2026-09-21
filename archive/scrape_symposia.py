#!/usr/bin/env python3
"""
Scrape book symposia from Crossref for existing PhilReviews journals.

A book symposium is a set of articles in the same journal issue, all discussing
the same book. Common structure:
  1. "Precis" or "Précis of [Book Title]" — by the book author
  2. Several commentary/critique articles
  3. "Replies" or "Response to Critics" — by the book author

Each contribution gets its own row with entry_type='symposium' and a shared
symposium_group linking them together.
"""

import re
import time
import requests
import sqlite3
import db

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'PhilReviews/1.0 (academic research; mailto:mzwolinski@sandiego.edu)',
})
CROSSREF_BASE = 'https://api.crossref.org/works'
RATE_LIMIT = 0.5  # seconds between requests


def get_existing_journals():
    """Get set of journal names currently in the database."""
    conn = sqlite3.connect(db.DB_PATH)
    rows = conn.execute(
        'SELECT DISTINCT publication_source FROM reviews'
    ).fetchall()
    conn.close()
    return {r[0] for r in rows if r[0]}


def crossref_get(params, timeout=30):
    """Make a Crossref API request with rate limiting."""
    params.setdefault('mailto', 'mzwolinski@sandiego.edu')
    time.sleep(RATE_LIMIT)
    resp = SESSION.get(CROSSREF_BASE, params=params, timeout=timeout)
    if resp.status_code != 200:
        return None
    return resp.json()


def find_precis_articles(journal_name):
    """Find all 'Precis' articles in a journal — these mark book symposia.

    Returns list of (crossref_item, book_title, book_author) tuples.
    """
    precis_hits = []

    for query in ['precis', 'précis']:
        data = crossref_get({
            'filter': f'container-title:{journal_name}',
            'query.title': query,
            'rows': 100,
        })
        if not data:
            continue

        for item in data.get('message', {}).get('items', []):
            title = (item.get('title', ['']) or [''])[0]
            title_lower = title.lower()

            # Must actually contain 'precis' or 'précis'
            if 'precis' not in title_lower and 'précis' not in title_lower:
                continue

            # Skip false positives (book reviews OF books about precis)
            if 'book review' in title_lower:
                continue

            # Extract book title from precis title
            # Patterns: "Precis of Book Title", "Precis: Book Title", "Précis", just "Precis"
            book_title = ''
            m = re.match(r'^(?:précis|precis)\s+(?:of\s+)?(.+)', title, re.IGNORECASE)
            if m:
                book_title = re.sub(r'<[^>]+>', '', m.group(1)).strip().rstrip('.')
            else:
                # Check for "<i>Book Title</i> precis" pattern
                m2 = re.match(r'^(?:<[^>]+>)?(.+?)(?:</[^>]+>)?\s+(?:précis|precis)', title, re.IGNORECASE)
                if m2:
                    book_title = re.sub(r'<[^>]+>', '', m2.group(1)).strip().rstrip('.')

            # Get the book author from the Crossref author field
            # (for precis articles, the author IS the book author)
            authors = item.get('author', [])
            book_author_first = authors[0].get('given', '') if authors else ''
            book_author_last = authors[0].get('family', '') if authors else ''

            precis_hits.append({
                'item': item,
                'book_title': book_title,
                'book_author_first': book_author_first,
                'book_author_last': book_author_last,
            })

    # Deduplicate by DOI
    seen = set()
    unique = []
    for hit in precis_hits:
        doi = hit['item'].get('DOI', '')
        if doi in seen:
            continue
        seen.add(doi)
        unique.append(hit)

    return unique


def find_symposium_markers(journal_name):
    """Find 'Book Symposium' or 'Symposium on' title markers in a journal."""
    markers = []

    for query in ['book symposium', 'symposium on', 'review symposium', 'critical discussion']:
        data = crossref_get({
            'filter': f'container-title:{journal_name}',
            'query.title': query,
            'rows': 50,
        })
        if not data:
            continue

        for item in data.get('message', {}).get('items', []):
            title = (item.get('title', ['']) or [''])[0]
            title_lower = title.lower()
            title_clean = re.sub(r'<[^>]+>', '', title_lower)

            # Must be an actual symposium marker, not a review of a book about symposia
            if 'book review' in title_clean:
                continue

            # Check for symposium patterns
            is_marker = False
            book_title = ''

            # "Book Symposium: Title" or "Book Symposium on Author's Title"
            m = re.match(r'^(?:book\s+)?symposium[:\s]+(?:on\s+)?(.+)', title_clean)
            if m:
                is_marker = True
                book_title = re.sub(r'<[^>]+>', '', m.group(1)).strip().rstrip('.')

            # "Review Symposium: Title"
            if not is_marker:
                m = re.match(r'^review\s+symposium[:\s]+(?:on\s+)?(.+)', title_clean)
                if m:
                    is_marker = True
                    book_title = re.sub(r'<[^>]+>', '', m.group(1)).strip().rstrip('.')

            # "Critical Discussion of Author's Title"
            if not is_marker:
                m = re.match(r'^critical\s+discussion[:\s]+(?:of\s+)?(.+)', title_clean)
                if m:
                    is_marker = True
                    book_title = re.sub(r'<[^>]+>', '', m.group(1)).strip().rstrip('.')

            # "Symposium on Author's Title" (but NOT "Symposium on topic" or "Symposium of Plato")
            if not is_marker:
                m = re.match(r'^symposium\s+on\s+(.+)', title_clean)
                if m:
                    rest = m.group(1)
                    # Likely a book symposium if it mentions an author's name (possessive)
                    if re.search(r"[a-z]+'s\s", rest) or re.search(r'[a-z]+\u2019s\s', rest):
                        is_marker = True
                        book_title = re.sub(r'<[^>]+>', '', rest).strip().rstrip('.')
                # "Symposium of X" is almost always a book ABOUT symposia, not a book symposium
                # Skip these

            if not is_marker:
                continue

            # Extract book author from the book title if it contains possessive
            book_author_first = ''
            book_author_last = ''
            pm = re.match(r"^([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-zA-Z-]+)['\u2019]s\s+(.+)", book_title)
            if pm:
                author_name = pm.group(1).strip()
                book_title = pm.group(2).strip()
                parts = author_name.split()
                book_author_first = ' '.join(parts[:-1])
                book_author_last = parts[-1]

            markers.append({
                'item': item,
                'book_title': book_title,
                'book_author_first': book_author_first,
                'book_author_last': book_author_last,
            })

    # Deduplicate by DOI
    seen = set()
    unique = []
    for hit in markers:
        doi = hit['item'].get('DOI', '')
        if doi in seen:
            continue
        seen.add(doi)
        unique.append(hit)

    return unique


def fetch_issue_articles(journal_name, year, volume, issue):
    """Fetch all articles from a specific journal issue via Crossref.

    Uses date-range filter + client-side volume/issue matching.
    """
    # Search with a date range around the year
    data = crossref_get({
        'filter': f'container-title:{journal_name},from-pub-date:{year}-01,until-pub-date:{year}-12',
        'rows': 200,
    })
    if not data:
        return []

    items = data.get('message', {}).get('items', [])
    matched = [i for i in items if i.get('volume') == volume and i.get('issue') == issue]
    return matched


def get_page_number(item):
    """Extract the starting page number from a Crossref item."""
    pages = item.get('page', '')
    if not pages:
        return 0
    first = pages.split('-')[0].strip()
    try:
        return int(first)
    except ValueError:
        return 0


def identify_symposium_articles(issue_articles, precis_item, book_title, book_author_last):
    """Given all articles in an issue and the precis article, identify which
    articles are part of the same book symposium.

    Strategy: Start from the precis page and include articles forward (and slightly
    backward) until we hit clear non-symposium content. The precis typically starts
    the symposium section; commentaries follow, then the author's replies.
    """
    precis_page = get_page_number(precis_item)
    if precis_page == 0:
        # Can't do page-based grouping; include articles with related titles
        return _identify_by_title(issue_articles, book_title, book_author_last, precis_item)

    # Sort articles by page number
    paged_articles = [(get_page_number(item), item) for item in issue_articles]
    paged_articles = [(p, item) for p, item in paged_articles if p > 0]
    paged_articles.sort(key=lambda x: x[0])

    book_author_lower = book_author_last.lower() if book_author_last else ''

    def is_non_symposium(item):
        """Check if an article is clearly not part of a symposium."""
        title = (item.get('title', ['']) or [''])[0]
        title_lower = title.lower()
        title_clean = re.sub(r'<[^>]+>', '', title_lower)

        # Front/back matter, errata, corrections
        if any(skip in title_clean for skip in [
            'issue information', 'front matter', 'back matter',
            'notes on contributors', 'book notes', 'book received',
            'recent publications', 'critical notices', 'recent work',
            'erratum', 'corrigendum', 'correction to:',
        ]):
            return True

        # Regular book reviews
        if title_clean.startswith('book review'):
            return True
        # Reviews with italic book titles followed by author (standard review format)
        if re.match(r'^<i>.+</i>\.\s+[A-Z]', title) and 'precis' not in title_lower:
            return True
        # Reviews with author-title format: "Author, <i>Title</i>"
        if re.match(r'^[A-Z][a-z]+.*?,\s*<i>', title):
            return True

        return False

    def is_reply(item):
        """Check if this looks like the book author's reply (ends the symposium)."""
        title = (item.get('title', ['']) or [''])[0].lower()
        title_clean = re.sub(r'<[^>]+>', '', title)
        return any(t in title_clean for t in [
            'reply to', 'replies', 'response to', 'author response',
            'reply to my critics', 'response to commentators',
            'response to contributors',
        ])

    symposium = []
    found_reply = False
    for page, item in paged_articles:
        # Include articles from precis page to ~80 pages after
        # But also include a few pages before (in case precis isn't first)
        if page < precis_page - 10:
            continue
        if page > precis_page + 80:
            break

        # If we already found the reply, stop
        if found_reply:
            break

        if is_non_symposium(item):
            # If we already have symposium articles and hit a non-symposium one,
            # we've reached the end of the symposium section
            if symposium and page > precis_page:
                break
            continue

        symposium.append(item)

        # Check if this is the reply — if so, include it but stop after
        if is_reply(item):
            found_reply = True

    return symposium


def _identify_by_title(issue_articles, book_title, book_author_last, precis_item):
    """Identify symposium articles by title matching (fallback when page numbers unavailable)."""
    symposium = [precis_item]

    book_title_norm = re.sub(r'[^a-z0-9 ]', '', book_title.lower()).strip() if book_title else ''
    author_lower = book_author_last.lower() if book_author_last else ''

    for item in issue_articles:
        if item.get('DOI') == precis_item.get('DOI'):
            continue

        title = (item.get('title', ['']) or [''])[0]
        title_lower = re.sub(r'<[^>]+>', '', title.lower())

        # Matches if title contains book title, author name, or symposium terms
        if book_title_norm and book_title_norm in title_lower:
            symposium.append(item)
        elif author_lower and len(author_lower) > 3 and author_lower in title_lower:
            symposium.append(item)
        elif any(t in title_lower for t in ['reply', 'replies', 'response to', 'symposium']):
            symposium.append(item)

    return symposium


def _infer_book_title(symposium_articles, book_author_last):
    """Try to extract the book title from symposium article titles.

    Looks for patterns like:
    - "Review of Author, <i>Book Title</i>"
    - "Critique of Author, <i>Book Title</i>"
    - "<i>Book Title</i>: Reply to My Critics"
    - "Reply to My Critics" in article by the book author
    """
    for item in symposium_articles:
        title = (item.get('title', ['']) or [''])[0]

        # Look for italic tags — they often contain the book title
        m = re.search(r'<i>(.+?)</i>', title)
        if m:
            candidate = m.group(1).strip().rstrip('.,:')
            # Must be substantial (>10 chars) to be a book title
            if len(candidate) > 10:
                return candidate

        # Look for "Reply to My Critics" pattern — the reply title sometimes has the book name
        title_clean = re.sub(r'<[^>]+>', '', title)
        m = re.match(r'^(.+?):\s*(?:reply|replies|response)', title_clean, re.IGNORECASE)
        if m:
            candidate = m.group(1).strip()
            if len(candidate) > 5:
                return candidate

    return ''


def build_records(articles, book_title, book_author_first, book_author_last, symposium_group, journal_name):
    """Convert Crossref items to database-ready symposium records."""
    records = []
    for item in articles:
        title = re.sub(r'<[^>]+>', '', (item.get('title', ['']) or [''])[0]).strip()
        doi = item.get('DOI', '')
        container = (item.get('container-title', ['']) or [''])[0]

        # Reviewer = this article's author
        authors = item.get('author', [])
        reviewer_first = authors[0].get('given', '') if authors else ''
        reviewer_last = authors[0].get('family', '') if authors else ''

        # Publication date
        pub_date = ''
        issued = item.get('issued', {})
        if issued.get('date-parts'):
            parts = issued['date-parts'][0]
            year = parts[0] if len(parts) >= 1 else 0
            month = parts[1] if len(parts) > 1 else 1
            day = parts[2] if len(parts) > 2 else 1
            if year:
                pub_date = f"{year:04d}-{month:02d}-{day:02d}"

        review_link = item.get('URL', '')
        access_type = 'Open' if item.get('license') else 'Restricted'

        records.append({
            'book_title': book_title or title,  # Use symposium book title, fallback to article title
            'book_author_first_name': book_author_first,
            'book_author_last_name': book_author_last,
            'reviewer_first_name': reviewer_first,
            'reviewer_last_name': reviewer_last,
            'publication_source': container or journal_name,
            'publication_date': pub_date,
            'review_link': review_link,
            'review_summary': title,  # Store article title as summary for context
            'access_type': access_type,
            'doi': doi,
            'entry_type': 'symposium',
            'symposium_group': symposium_group,
        })

    return records


def process_symposium(hit, journal_name):
    """Process a single symposium hit: find all articles in the same issue, group them.

    Returns list of records to insert.
    """
    item = hit['item']
    book_title = hit['book_title']
    book_author_first = hit['book_author_first']
    book_author_last = hit['book_author_last']

    volume = item.get('volume', '')
    issue = item.get('issue', '')
    if not volume or not issue:
        return []

    year = ''
    issued = item.get('issued', {})
    if issued.get('date-parts'):
        year = str(issued['date-parts'][0][0])

    if not year:
        return []

    symposium_group = f"{journal_name}|{year}|{volume}|{issue}"

    # Fetch all articles in the same issue
    issue_articles = fetch_issue_articles(journal_name, year, volume, issue)
    if not issue_articles:
        # Fall back to just the marker article itself
        return build_records([item], book_title, book_author_first, book_author_last,
                             symposium_group, journal_name)

    # Identify symposium articles
    symposium_articles = identify_symposium_articles(
        issue_articles, item, book_title, book_author_last
    )

    if len(symposium_articles) < 2:
        # Not a real symposium if there's only one article
        return []

    # If book_title is empty, try to extract it from the symposium articles
    if not book_title:
        book_title = _infer_book_title(symposium_articles, book_author_last)

    # If still no book title, use author name as fallback
    if not book_title and book_author_last:
        book_title = f"(symposium on work by {book_author_first} {book_author_last})"

    # Validate: at least some articles should contain symposium-related terms
    # (precis, reply, response, author name, book title). Otherwise it's just
    # regular articles near each other by page proximity.
    author_lower = book_author_last.lower() if book_author_last else ''
    book_lower = book_title.lower() if book_title else ''
    # Strip fallback text from book title for matching
    if book_lower.startswith('(symposium on work by'):
        book_lower = ''
    refs_found = 0
    for art in symposium_articles:
        art_title = re.sub(r'<[^>]+>', '', (art.get('title', [''])[0] or '')).lower()
        if author_lower and author_lower in art_title:
            refs_found += 1
        elif book_lower and len(book_lower) > 10 and book_lower in art_title:
            refs_found += 1
        elif any(t in art_title for t in ['precis', 'précis', 'reply', 'replies', 'response to',
                                           'symposium', 'critical discussion', 'book forum']):
            refs_found += 1
    # If fewer than 30% of articles have symposium markers, likely not a real symposium
    if refs_found < max(2, len(symposium_articles) * 0.3):
        return []

    return build_records(symposium_articles, book_title, book_author_first, book_author_last,
                         symposium_group, journal_name)


def scrape_journal(journal_name):
    """Find and process book symposia for a single journal.

    Returns list of records to insert.
    """
    print(f"\n=== {journal_name} ===")

    # Strategy 1: Find 'Precis' articles
    precis_hits = find_precis_articles(journal_name)
    # Strategy 2: Find symposium marker titles
    marker_hits = find_symposium_markers(journal_name)

    # Merge, deduplicate by DOI
    all_hits = {}
    for hit in precis_hits + marker_hits:
        doi = hit['item'].get('DOI', '')
        if doi and doi not in all_hits:
            all_hits[doi] = hit

    if not all_hits:
        print("  No symposium markers found")
        return []

    print(f"  Found {len(all_hits)} symposium markers (precis: {len(precis_hits)}, other: {len(marker_hits)})")

    all_records = []
    seen_groups = set()

    for doi, hit in sorted(all_hits.items()):
        item = hit['item']
        vol = item.get('volume', '')
        iss = item.get('issue', '')
        group_key = f"{journal_name}|{vol}|{iss}"

        # Skip if we already processed this issue
        if group_key in seen_groups:
            continue
        seen_groups.add(group_key)

        records = process_symposium(hit, journal_name)
        if records:
            book = records[0]['book_title'][:50]
            contribs = ', '.join(
                f"{r['reviewer_first_name']} {r['reviewer_last_name']}".strip()
                for r in records
            )
            year = item.get('issued', {}).get('date-parts', [[0]])[0][0]
            print(f"  {year} v{vol}i{iss}: {book} ({len(records)} contributions)")
            print(f"    {contribs[:100]}")
            all_records.extend(records)

    return all_records


def insert_records(records):
    """Insert symposium records into the database, skipping existing ones."""
    existing = 0
    new_records = []
    for r in records:
        if r['doi'] and db.doi_exists(r['doi']):
            existing += 1
        elif r['review_link'] and db.review_link_exists(r['review_link']):
            existing += 1
        else:
            new_records.append(r)

    print(f"\nAlready in DB: {existing}")
    print(f"New to insert: {len(new_records)}")

    if new_records:
        db.insert_reviews(new_records)
        print(f"Inserted {len(new_records)} symposium contributions")

        # Count unique symposia
        groups = set(r['symposium_group'] for r in new_records)
        print(f"Across {len(groups)} symposia")

    return new_records


def scrape_all():
    """Scrape symposia from all existing journals."""
    journals = sorted(get_existing_journals())
    print(f"Checking {len(journals)} journals for book symposia...\n")

    all_records = []
    for journal in journals:
        records = scrape_journal(journal)
        all_records.extend(records)

    print(f"\n{'='*60}")
    print(f"Total symposium contributions found: {len(all_records)}")

    if all_records:
        insert_records(all_records)

    return all_records


def scrape_one(journal_name):
    """Scrape symposia from a single journal (for testing)."""
    records = scrape_journal(journal_name)
    print(f"\nFound {len(records)} symposium contributions total")

    if not records:
        return records

    for r in records:
        rv = f"{r['reviewer_first_name']} {r['reviewer_last_name']}".strip() or '-'
        print(f"  {r['book_title'][:40]} | {rv} | {r['review_summary'][:40]}")

    # Ask before inserting
    print(f"\nInsert {len(records)} records? (y/n)")
    if input().strip().lower() == 'y':
        insert_records(records)

    return records


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        scrape_one(sys.argv[1])
    else:
        scrape_all()
