#!/usr/bin/env python3
"""
Scrape Philosophy in Review from OJS archive (journals.uvic.ca).
Crossref only covers 2020+; this gets the full 1981-present archive.
"""

import re
import time
import requests
import db

ARCHIVE_URL = 'https://journals.uvic.ca/index.php/pir/issue/archive'
ISSUE_URL = 'https://journals.uvic.ca/index.php/pir/issue/view/{}'
SESSION = requests.Session()
SESSION.headers['User-Agent'] = 'PhilReviews/1.0 (mzwolinski@sandiego.edu)'


def get_issue_ids():
    """Fetch all issue IDs from the archive page."""
    resp = SESSION.get(ARCHIVE_URL, timeout=30)
    resp.raise_for_status()
    ids = re.findall(r'/pir/issue/view/(\d+)', resp.text)
    return list(dict.fromkeys(ids))  # deduplicate, preserve order


def parse_issue(issue_id):
    """Scrape one issue page and return review records."""
    resp = SESSION.get(ISSUE_URL.format(issue_id), timeout=30)
    resp.raise_for_status()
    html = resp.text

    # Extract volume/issue/date from page title
    date_match = re.search(
        r'Vol\.\s*(\d+)\s*No\.\s*[\d/]+\s*\((\d{4})\)', html
    )
    year = date_match.group(2) if date_match else ''
    pub_date = f'{year}-01-01' if year else ''

    # Find all article summary blocks
    blocks = re.findall(
        r'<div class="obj_article_summary">(.*?)</div>\s*(?:</div>|\s*</li>)',
        html, re.DOTALL
    )

    records = []
    for block in blocks:
        # Extract title text from <h3 class="title"><a ...>TEXT</a></h3>
        title_match = re.search(
            r'<h3 class="title">\s*<a[^>]*>\s*(.*?)\s*</a>',
            block, re.DOTALL
        )
        if not title_match:
            continue
        raw_title = title_match.group(1).strip()
        raw_title = re.sub(r'\s+', ' ', raw_title)

        # Skip "Full Issue" entries
        if raw_title.lower().startswith('full issue'):
            continue

        # Extract reviewer from <div class="authors">
        reviewer_match = re.search(
            r'<div class="authors">\s*(.*?)\s*</div>', block, re.DOTALL
        )
        reviewer_name = reviewer_match.group(1).strip() if reviewer_match else ''
        reviewer_name = re.sub(r'<[^>]+>', '', reviewer_name).strip()

        # Extract article link
        link_match = re.search(r'href="(https://journals\.uvic\.ca/[^"]+/article/view/\d+)"', block)
        link = link_match.group(1) if link_match else ''

        # Parse the title: Author, "Title" or Author, 'Title'
        quoted = re.match(
            r'^(.+?),?\s*(?:\([Ee]ds?\.?\)\s*\.?\s*,?\s*)?'
            r'["\u0027\u201c\u2018](.{5,}?)["\u0027\u201d\u2019]\.?\s*$',
            raw_title
        )

        if quoted:
            author_str = quoted.group(1).strip().rstrip(',').strip()
            author_str = re.sub(r',?\s*\([Ee]ds?\.?\)\s*\.?', '', author_str).strip()
            author_str = author_str.rstrip(',').strip()
            book_title = quoted.group(2).strip().rstrip('.')
        else:
            # Fallback: use entire text as title, no author
            book_title = raw_title
            author_str = ''

        # Split author into first/last
        book_first, book_last = '', ''
        if author_str:
            parts = author_str.rsplit(' ', 1)
            if len(parts) == 2:
                book_first, book_last = parts
            else:
                book_last = parts[0]

        # Split reviewer into first/last
        rev_first, rev_last = '', ''
        if reviewer_name:
            parts = reviewer_name.rsplit(' ', 1)
            if len(parts) == 2:
                rev_first, rev_last = parts
            else:
                rev_last = parts[0]

        records.append({
            'book_title': book_title,
            'book_author_first_name': book_first,
            'book_author_last_name': book_last,
            'reviewer_first_name': rev_first,
            'reviewer_last_name': rev_last,
            'publication_source': 'Philosophy in Review',
            'publication_date': pub_date,
            'review_link': link,
            'review_summary': '',
            'access_type': 'Open',
            'doi': '',
        })

    return records


def main():
    issue_ids = get_issue_ids()
    print(f'Found {len(issue_ids)} issues')

    all_records = []
    for i, iid in enumerate(issue_ids):
        records = parse_issue(iid)
        all_records.extend(records)
        if (i + 1) % 20 == 0 or i == len(issue_ids) - 1:
            print(f'  Scraped {i + 1}/{len(issue_ids)} issues, {len(all_records)} reviews so far')
        time.sleep(0.3)

    print(f'\nTotal reviews scraped: {len(all_records)}')

    # Check for existing entries by review_link
    new_records = []
    for r in all_records:
        if r['review_link'] and not db.review_link_exists(r['review_link']):
            new_records.append(r)
        elif not r['review_link']:
            new_records.append(r)

    print(f'New records (not already in DB): {len(new_records)}')

    if new_records:
        db.insert_reviews(new_records)
        print(f'Inserted {len(new_records)} records')

    # Print samples
    for r in new_records[:5]:
        print(f'  "{r["book_title"][:50]}" by {r["book_author_first_name"]} {r["book_author_last_name"]} | reviewed by {r["reviewer_first_name"]} {r["reviewer_last_name"]}')


if __name__ == '__main__':
    main()
