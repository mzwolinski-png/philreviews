#!/usr/bin/env python3
"""
Scrape Reason Papers book reviews from reasonpapers.com/archives/.
"""

import re
import requests
import db

ARCHIVE_URL = 'https://reasonpapers.com/archives/'


def scrape():
    resp = requests.get(ARCHIVE_URL, timeout=30)
    resp.raise_for_status()
    html = resp.text

    # Find all <li> entries that look like reviews
    # Format: <a href="PDF">Author's <em>Title</em></a> —Reviewer
    # Also: <a href="PDF">Review of Author's <em>Title</em></a> —Reviewer
    # Also: <a href="PDF">Review Essay: Author's <em>Title</em></a> —Reviewer
    entries = re.findall(r'<li[^>]*>\s*(?:<span[^>]*>)?\s*<a\s+href="([^"]+)"[^>]*>(.*?)</a>\s*(?:</span>)?\s*(?:&#?8212;|—|–|-)\s*(.*?)\s*</li>', html, re.DOTALL)

    # Also need to figure out which issue each entry belongs to
    # Parse the full HTML to get issue context
    # Issues are typically marked with headers like <h3> or <strong> containing volume info

    records = []
    current_year = ''
    in_symposium = False

    # Split by lines and track context
    lines = html.split('\n')
    for line in lines:
        # Check for year/issue markers
        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', line)
        if re.search(r'<h[23456]|<strong|Issue\s+\d|Vol(?:ume)?\.?\s+\d', line, re.IGNORECASE):
            if year_match:
                current_year = year_match.group(1)

        # Track symposium sections — skip entries inside them
        if re.search(r'Symposium:', line, re.IGNORECASE):
            in_symposium = True
        if re.search(r'(?:Book Review|Review Essay|<h[23456]|<strong>Issue|<strong>Vol)', line, re.IGNORECASE) and not re.search(r'Symposium', line, re.IGNORECASE):
            in_symposium = False

        # Check for review entries
        entry_match = re.search(
            r'<a\s+href="([^"]+)"[^>]*>(.*?)</a>\s*(?:(?:<br\s*/?>)?\s*(?:&#?8212;|—|–|\s-)\s*(.*?))?</li>',
            line, re.DOTALL
        )
        if not entry_match:
            continue

        link = entry_match.group(1).strip()
        title_html = entry_match.group(2).strip()
        reviewer_name = entry_match.group(3).strip() if entry_match.group(3) else ''

        # Clean HTML from reviewer
        reviewer_name = re.sub(r'<[^>]+>', '', reviewer_name).strip()
        reviewer_name = reviewer_name.strip('—–- \t')

        # Skip non-review entries (articles, symposium pieces, editor's notes)
        title_text = re.sub(r'<[^>]+>', '', title_html).strip()
        title_lower = title_text.lower()

        # Must have <em> tag (italic book title) to be a book review
        has_em = '<em>' in title_html
        if not has_em:
            continue

        # Skip entries inside symposium sections (discussions, not standalone reviews)
        if in_symposium:
            continue

        # Skip entries where the italic text is "Reason Papers" (retrospective articles)
        em_text_check = re.search(r'<em>(.*?)</em>', title_html)
        if em_text_check and em_text_check.group(1).strip().lower() == 'reason papers':
            continue

        # Skip things that are clearly not book reviews
        if any(skip in title_lower for skip in ['editor', 'introduction', 'afterword',
                                                  'symposium', 'rejoinder', 'reply to',
                                                  'response to', 'preface', 'reflecting on']):
            continue

        # Extract book title from <em> tags
        em_match = re.search(r'<em>(.*?)</em>', title_html)
        book_title = em_match.group(1).strip() if em_match else ''

        # Extract book author from text before <em>
        pre_em = re.sub(r'<[^>]+>', '', title_html[:title_html.index('<em>')]).strip() if em_match else ''
        # Remove "Review of" / "Review Essay:" prefix
        pre_em = re.sub(r'^(?:Review\s+(?:Essay)?:?\s*(?:of\s+)?)', '', pre_em, flags=re.IGNORECASE).strip()

        # Author is usually "Name's" (possessive)
        author_str = ''
        if pre_em:
            poss_match = re.match(r"^(.+?)(?:'s|&#8217;s|\u2019s)\s*$", pre_em)
            if poss_match:
                author_str = poss_match.group(1).strip()
            else:
                # Might be "Author (ed.)" or just author name
                author_str = re.sub(r'\s*\(eds?\.?\)\s*', '', pre_em).strip().rstrip("'").strip()

        if not book_title:
            continue

        # Clean book title
        book_title = re.sub(r'<[^>]+>', '', book_title).strip()
        book_title = book_title.rstrip('.')

        # Make link absolute
        if link.startswith('/'):
            link = 'https://reasonpapers.com' + link

        # Split author into first/last
        book_first, book_last = '', ''
        if author_str:
            # Handle "(ed.)" in author
            author_str = re.sub(r'\s*\(eds?\.?\)\s*', '', author_str).strip()
            parts = author_str.rsplit(' ', 1)
            if len(parts) == 2:
                book_first, book_last = parts
            else:
                book_last = parts[0]

        # Split reviewer
        rev_first, rev_last = '', ''
        if reviewer_name:
            parts = reviewer_name.rsplit(' ', 1)
            if len(parts) == 2:
                rev_first, rev_last = parts
            else:
                rev_last = parts[0]

        pub_date = f'{current_year}-01-01' if current_year else ''

        records.append({
            'book_title': book_title,
            'book_author_first_name': book_first,
            'book_author_last_name': book_last,
            'reviewer_first_name': rev_first,
            'reviewer_last_name': rev_last,
            'publication_source': 'Reason Papers',
            'publication_date': pub_date,
            'review_link': link,
            'review_summary': '',
            'access_type': 'Open',
            'doi': '',
        })

    return records


def main():
    records = scrape()
    print(f'Scraped {len(records)} reviews')

    # Deduplicate by link
    new = []
    for r in records:
        if r['review_link'] and not db.review_link_exists(r['review_link']):
            new.append(r)
        elif not r['review_link']:
            new.append(r)

    print(f'New records: {len(new)}')

    if new:
        db.insert_reviews(new)
        print(f'Inserted {len(new)} records')

    for r in new[:10]:
        print(f'  "{r["book_title"][:50]}" by {r["book_author_first_name"]} {r["book_author_last_name"]} | reviewed by {r["reviewer_first_name"]} {r["reviewer_last_name"]} ({r["publication_date"][:4]})')


if __name__ == '__main__':
    main()
