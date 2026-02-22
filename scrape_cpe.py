#!/usr/bin/env python3
"""Scrape book reviews from Constitutional Political Economy via IDEAS/RePEc."""

import re
import time
import requests
from bs4 import BeautifulSoup
import db

SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'PhilReviews/1.0 (academic research)'})

BASE_URL = 'https://ideas.repec.org/s/kap'
PAGES = ['copoec.html', 'copoec2.html', 'copoec3.html', 'copoec4.html', 'copoec5.html']


def is_book_review(title):
    """Check if an article title looks like a book review."""
    # Book reviews typically contain publisher info, page counts, ISBNs, or year in parens
    indicators = [
        r'\b(?:University Press|Cambridge|Oxford|Springer|Palgrave|Routledge|MIT Press)\b',
        r'\bpp\.\s*\d+',
        r'\b978[-\d]+',  # ISBN
        r'\bby\s+[A-Z][a-z]+\s+[A-Z]',  # "by Author Name"
        r'\(\d{4}\)',  # Year in parens
        r'(?:New York|London|Princeton|Chicago|Cambridge):\s',  # City: Publisher
    ]
    for pat in indicators:
        if re.search(pat, title, re.IGNORECASE):
            return True
    return False


def parse_book_review(title):
    """Parse a book review title to extract book title and author."""
    # Common patterns:
    # "Book Title by Author Name. Publisher, Year. pp.XXX"
    # "Book Title by Author. City: Publisher Year"

    # Try to extract book title (before "by") and author
    # Pattern: "Book Title by Author Name. Publisher..."
    m = re.match(r'^(.+?)\s+by\s+(.+?)(?:\.\s+(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*:?\s+)?(?:Cambridge|Oxford|Princeton|Springer|Palgrave|Routledge|MIT|University|Press|New York|London|Chicago|Elgar|Wiley|Sage|Academic|Harper|Norton|Penguin|Random|Polity|Verso|Beacon|Basic|Free|Harvard|Yale|Stanford|Columbia|Duke|Cornell|Michigan|Indiana|Minnesota|Illinois|Wisconsin|Johns Hopkins|Georgetown|Notre Dame|Rowman|Lexington|Transaction|Liberty Fund|Cato|Brookings|AEI|Hoover).+$)', title, re.IGNORECASE)
    if m:
        book_title = m.group(1).strip().rstrip(',.')
        author_str = m.group(2).strip().rstrip(',.')
        return book_title, author_str

    # Pattern: "Book Title by Author Name. Year" or "Book Title by Author Name, Publisher Year"
    m = re.match(r'^(.+?)\s+by\s+(.+?)(?:[,.]?\s+(?:19|20)\d{2})', title, re.IGNORECASE)
    if m:
        book_title = m.group(1).strip().rstrip(',.')
        author_str = m.group(2).strip().rstrip(',.')
        # Remove any publisher info from author string
        author_str = re.sub(r'\.\s*(?:Cambridge|Oxford|Princeton|Springer|Palgrave|New York|London).*$', '', author_str, flags=re.IGNORECASE).strip().rstrip('.')
        return book_title, author_str

    # Pattern: "Book Title. by Author Name"
    m = re.match(r'^(.+?)\.\s+by\s+(.+?)(?:\.\s|,\s|$)', title, re.IGNORECASE)
    if m:
        book_title = m.group(1).strip()
        author_str = m.group(2).strip().rstrip(',.')
        return book_title, author_str

    # If no "by" pattern, try "Book Title, Author Name, Publisher"
    # Less reliable, skip for now

    return title, ''


def extract_first_author(author_str):
    """Extract first author's first and last name from an author string."""
    if not author_str:
        return '', ''

    # Remove editor indicators
    author_str = re.sub(r'\s*\(eds?\.\)\s*', '', author_str, flags=re.IGNORECASE)
    author_str = re.sub(r'\s*\(editors?\)\s*', '', author_str, flags=re.IGNORECASE)
    author_str = re.sub(r'\s*,?\s*editors?\s*$', '', author_str, flags=re.IGNORECASE)

    # Take first author (before "and", "&", ",")
    first_author = re.split(r'\s+and\s+|\s*&\s*|\s*,\s*(?=[A-Z])', author_str)[0].strip()

    parts = first_author.split()
    if len(parts) >= 2:
        return ' '.join(parts[:-1]), parts[-1]
    elif len(parts) == 1:
        return '', parts[0]
    return '', ''


def scrape_page(url):
    """Scrape a single IDEAS/RePEc page for book reviews."""
    resp = SESSION.get(url, timeout=15)
    if resp.status_code != 200:
        print(f"  Failed to fetch {url}: HTTP {resp.status_code}")
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')
    records = []

    # Find all article entries - they're in <li> tags within the main content
    # Each entry has a link with the article title
    for li in soup.find_all('li'):
        # Find the link with the article title
        link = li.find('a')
        if not link:
            continue

        title = link.get_text(strip=True)
        href = link.get('href', '')

        if not is_book_review(title):
            continue

        # Get reviewer name from the "by" text after the link
        li_text = li.get_text(strip=True)
        # The reviewer name comes after the title, usually after "by"
        # Format: "pp-pp Title by ReviewerName"
        reviewer_str = ''
        # Find text after the link
        for sibling in link.next_siblings:
            text = sibling.string if hasattr(sibling, 'string') and sibling.string else str(sibling)
            if text and text.strip():
                reviewer_str += text.strip()

        # Clean reviewer string - remove "by " prefix
        reviewer_str = re.sub(r'^[\s,]*by\s+', '', reviewer_str, flags=re.IGNORECASE).strip()
        # Remove trailing parens with dates
        reviewer_str = re.sub(r'\s*\(.*?\)\s*$', '', reviewer_str).strip()

        # Parse the book title and book author from the article title
        book_title, book_author_str = parse_book_review(title)
        book_author_first, book_author_last = extract_first_author(book_author_str)

        # Parse reviewer name
        reviewer_first, reviewer_last = extract_first_author(reviewer_str)

        # Extract year from the section heading
        year = ''
        parent = li.parent
        if parent:
            prev = parent.find_previous(['h3', 'h2', 'h4', 'strong'])
            if prev:
                m = re.search(r'((?:19|20)\d{2})', prev.get_text())
                if m:
                    year = m.group(1)

        # Build review link
        review_link = ''
        if href:
            if href.startswith('/'):
                review_link = f'https://ideas.repec.org{href}'
            elif href.startswith('http'):
                review_link = href

        record = {
            'book_title': book_title,
            'book_author_first_name': book_author_first,
            'book_author_last_name': book_author_last,
            'reviewer_first_name': reviewer_first,
            'reviewer_last_name': reviewer_last,
            'publication_source': 'Constitutional Political Economy',
            'publication_date': f'{year}-01-01' if year else '',
            'review_link': review_link,
            'review_summary': '',
            'access_type': 'Restricted',
            'doi': '',
        }
        records.append(record)

    return records


def scrape_all():
    """Scrape all book reviews from Constitutional Political Economy."""
    all_records = []

    for page in PAGES:
        url = f'{BASE_URL}/{page}'
        print(f"Scraping {url}...")
        records = scrape_page(url)
        print(f"  Found {len(records)} book reviews")
        for r in records:
            ba = f"{r['book_author_first_name']} {r['book_author_last_name']}".strip() or '-'
            rv = f"{r['reviewer_first_name']} {r['reviewer_last_name']}".strip() or '-'
            print(f"    {r['book_title'][:55]} | author: {ba} | reviewer: {rv}")
        all_records.extend(records)
        time.sleep(1)

    print(f"\n{'='*60}")
    print(f"Total reviews found: {len(all_records)}")

    if all_records:
        # Check for duplicates
        existing = 0
        new_records = []
        for r in all_records:
            if r['review_link'] and db.review_link_exists(r['review_link']):
                existing += 1
            else:
                new_records.append(r)

        print(f"Already in DB: {existing}")
        print(f"New to insert: {len(new_records)}")

        if new_records:
            db.insert_reviews(new_records)
            print(f"Inserted {len(new_records)} reviews")

    return all_records


if __name__ == '__main__':
    scrape_all()
