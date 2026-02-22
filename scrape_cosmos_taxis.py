#!/usr/bin/env python3
"""Scrape book reviews from Cosmos + Taxis journal website."""

import re
import time
import requests
from bs4 import BeautifulSoup
import db

SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'PhilReviews/1.0 (academic research)'})

BASE_URL = 'https://cosmosandtaxis.org'

# Year pages and their volume numbers
YEARS = [
    (2013, 1), (2014, 2), (2015, 3), (2016, 4), (2017, 5), (2018, 6),
    (2019, 7), (2020, 8), (2021, 9), (2022, 10), (2023, 11), (2024, 12),
    (2025, 13), (2026, 14),
]


def get_issue_urls(year):
    """Get all issue page URLs for a given year."""
    url = f'{BASE_URL}/{year}-2/'
    resp = SESSION.get(url, timeout=15)
    if resp.status_code != 200:
        print(f"  Failed to fetch year page {year}: HTTP {resp.status_code}")
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')
    issue_urls = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '/ct-' in href and href.startswith(BASE_URL):
            if href not in issue_urls:
                issue_urls.append(href)

    return issue_urls


def scrape_issue(url, year):
    """Scrape book reviews from a single issue page."""
    resp = SESSION.get(url, timeout=15)
    if resp.status_code != 200:
        print(f"  Failed to fetch {url}: HTTP {resp.status_code}")
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')
    text = soup.get_text()
    html = resp.text

    # Find the REVIEWS section
    # Look for a heading containing "REVIEW" (could be "REVIEWS", "REVIEW ESSAYS", etc.)
    reviews_section = None

    # Strategy: find all headings, locate the REVIEW heading, grab content after it
    headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'strong'])
    review_heading_idx = None
    next_section_idx = None

    for i, h in enumerate(headings):
        h_text = h.get_text(strip=True).upper()
        if 'REVIEW' in h_text and h_text not in ('PEER REVIEW', 'PEER-REVIEWED'):
            review_heading_idx = i
        elif review_heading_idx is not None and h_text in (
            'ARTICLES', 'SYMPOSIUM', 'ESSAYS', 'EDITORIAL', 'INTRODUCTION',
            'SPECIAL ISSUE', 'ABOUT', 'SHARE THIS', 'RELATED',
        ):
            next_section_idx = i
            break

    if review_heading_idx is None:
        return []

    # Get all content between review heading and next section (or end)
    review_heading = headings[review_heading_idx]

    # Walk through siblings and descendants after the review heading
    reviews = []

    # Alternative approach: parse the raw HTML between REVIEW heading and next section
    # Find the position of "REVIEW" in the HTML
    review_patterns = [
        r'<h[1-6][^>]*>\s*REVIEWS?\s*</h[1-6]>',
        r'<h[1-6][^>]*>\s*REVIEW\s+ESSAYS?\s*</h[1-6]>',
        r'<p[^>]*>\s*<strong>\s*REVIEWS?\s*</strong>\s*</p>',
    ]

    review_start = None
    for pat in review_patterns:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            review_start = m.end()
            break

    if review_start is None:
        # Try a broader search
        m = re.search(r'REVIEWS?</(?:h[1-6]|strong|b)>', html, re.IGNORECASE)
        if m:
            review_start = m.end()

    if review_start is None:
        return []

    # Find the end of the review section (next major heading or end of content)
    review_html = html[review_start:]

    # Cut at next major section heading
    end_patterns = [
        r'<h[1-6][^>]*>\s*(?:ARTICLES?|SYMPOSIUM|ESSAYS|EDITORIAL)',
        r'<div\s+class="(?:sharedaddy|entry-footer|wp-block-group)',
    ]
    for pat in end_patterns:
        m = re.search(pat, review_html, re.IGNORECASE)
        if m:
            review_html = review_html[:m.start()]
            break

    # Parse individual review entries from the review section
    review_soup = BeautifulSoup(review_html, 'html.parser')

    # Look for PDF links - each review typically has a PDF link
    pdf_links = review_soup.find_all('a', href=lambda h: h and '.pdf' in h.lower())

    if not pdf_links:
        # Try to find entries by <em> tags (italic book titles)
        em_tags = review_soup.find_all('em')
        for em in em_tags:
            book_title = em.get_text(strip=True)
            if len(book_title) < 5:
                continue
            # Find the reviewer name nearby (usually in <strong> or plain text after)
            parent = em.parent
            if parent:
                full_text = parent.get_text(strip=True)
                # Remove the book title to find reviewer
                remainder = full_text.replace(book_title, '').strip()
                # Clean up common patterns
                remainder = re.sub(r'^[\s,\-–—:]+', '', remainder)
                remainder = re.sub(r'[\s,\-–—:]+$', '', remainder)
                if remainder and len(remainder) < 60:
                    reviews.append({
                        'book_title': book_title,
                        'reviewer': remainder,
                        'link': '',
                    })
        return _process_reviews(reviews, year)

    # Process PDF links - the text near each link contains the review info
    for pdf_link in pdf_links:
        link_url = pdf_link['href']

        # Get the parent paragraph/div
        parent = pdf_link.find_parent(['p', 'div', 'li'])
        if not parent:
            parent = pdf_link.parent

        if not parent:
            continue

        full_text = parent.get_text(strip=True)

        # Find italic text (book title)
        em = parent.find('em') or parent.find('i')
        if em:
            book_title = em.get_text(strip=True)
        else:
            book_title = full_text

        # Find bold text (reviewer name) or text after the title
        strong = parent.find('strong') or parent.find('b')
        if strong:
            reviewer = strong.get_text(strip=True)
        else:
            # Reviewer name is usually after the book title
            remainder = full_text.replace(book_title, '').strip()
            remainder = re.sub(r'^[\s,\-–—:]+', '', remainder)
            remainder = re.sub(r'[\s,\-–—:]+$', '', remainder)
            reviewer = remainder

        if book_title and len(book_title) > 3:
            reviews.append({
                'book_title': book_title,
                'reviewer': reviewer,
                'link': link_url,
            })

    return _process_reviews(reviews, year)


def _process_reviews(raw_reviews, year):
    """Convert raw review dicts to database-ready records."""
    records = []
    for r in raw_reviews:
        book_title = r['book_title'].strip()
        reviewer_name = r['reviewer'].strip()

        # Skip non-review entries
        if not book_title or len(book_title) < 5:
            continue

        # Clean reviewer name
        reviewer_name = re.sub(r'^(reviewed\s+)?by\s+', '', reviewer_name, flags=re.IGNORECASE)
        reviewer_name = re.sub(r'\s*\(.*?\)\s*$', '', reviewer_name)  # Remove parenthetical
        reviewer_name = reviewer_name.strip().rstrip('.')

        # Split reviewer into first/last
        parts = reviewer_name.split()
        if len(parts) >= 2:
            reviewer_first = ' '.join(parts[:-1])
            reviewer_last = parts[-1]
        elif len(parts) == 1:
            reviewer_first = ''
            reviewer_last = parts[0]
        else:
            reviewer_first = ''
            reviewer_last = ''

        # Try to extract book author from title if present
        # Common patterns: "Book Title by Author Name" or "Author Name, Book Title"
        book_author_first = ''
        book_author_last = ''

        # Check for "by Author" at end of title
        m = re.match(r'^(.+?)\s+by\s+([A-Z][a-zA-Z.\s-]+)$', book_title)
        if m:
            book_title = m.group(1).strip()
            author_str = m.group(2).strip()
            author_parts = author_str.split()
            if len(author_parts) >= 2:
                book_author_first = ' '.join(author_parts[:-1])
                book_author_last = author_parts[-1]

        record = {
            'book_title': book_title,
            'book_author_first_name': book_author_first,
            'book_author_last_name': book_author_last,
            'reviewer_first_name': reviewer_first,
            'reviewer_last_name': reviewer_last,
            'publication_source': 'Cosmos + Taxis',
            'publication_date': f'{year}-01-01',
            'review_link': r.get('link', ''),
            'review_summary': '',
            'access_type': 'Open',
            'doi': '',
        }
        records.append(record)

    return records


def scrape_all():
    """Scrape all book reviews from Cosmos + Taxis."""
    all_records = []

    for year, vol in YEARS:
        print(f"\n=== {year} (Volume {vol}) ===")
        issue_urls = get_issue_urls(year)
        if not issue_urls:
            print(f"  No issues found")
            continue

        print(f"  Found {len(issue_urls)} issues")

        for url in issue_urls:
            time.sleep(0.5)
            reviews = scrape_issue(url, year)
            if reviews:
                print(f"  {url.split('/')[-2]}: {len(reviews)} reviews")
                for r in reviews:
                    print(f"    - {r['book_title'][:60]} | reviewer: {r['reviewer_first_name']} {r['reviewer_last_name']}")
                all_records.extend(reviews)

        time.sleep(0.5)

    print(f"\n{'='*60}")
    print(f"Total reviews found: {len(all_records)}")

    if all_records:
        # Check for duplicates against existing DB
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
