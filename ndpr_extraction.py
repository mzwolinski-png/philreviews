#!/usr/bin/env python3
"""
Shared NDPR extraction module.
Uses CSS selectors based on NDPR's consistent HTML structure to reliably
extract review data from Notre Dame Philosophical Reviews pages.
"""

import re
from bs4 import BeautifulSoup, NavigableString
from datetime import datetime
from typing import Dict, Optional, Tuple, List


def is_review_page(soup: BeautifulSoup) -> bool:
    """Check if a parsed page is an actual NDPR review (not an archive, index, etc.)."""
    return soup.select_one('div.meta-item__bibliography') is not None


def extract_review_data(soup: BeautifulSoup, url: str) -> Optional[Dict]:
    """
    Extract all review data from a parsed NDPR review page using CSS selectors.

    Returns a dict with keys matching the Airtable schema:
        book_title, book_author_first, book_author_last,
        reviewer_first, reviewer_last, publication_date,
        review_link, review_summary, publication_source,
        access_type, doi,
        _metadata (is_edited_volume, has_multiple_authors, has_multiple_reviewers,
                   reviewer_affiliation, full_bibliography)
    """
    if not is_review_page(soup):
        return None

    result = {
        'review_link': url,
        'publication_source': 'Notre Dame Philosophical Reviews',
        'access_type': 'Open',
        '_metadata': {}
    }

    # --- Book title ---
    # Primary: from <em> in the bibliography (this is the actual book title)
    # Fallback: from the h1 heading
    bib_div = soup.select_one('div.meta-item__bibliography')
    em_tag = bib_div.find('em') if bib_div else None

    if em_tag:
        result['book_title'] = _clean_text(em_tag.get_text())
    else:
        h1 = soup.select_one('h1.article-header__title')
        if h1:
            result['book_title'] = _clean_text(h1.get_text())

    if not result.get('book_title'):
        return None

    # --- Bibliography parsing (author, publisher info) ---
    if bib_div:
        bib_p = bib_div.find('p')
        if bib_p:
            full_bib = _clean_text(bib_p.get_text())
            result['_metadata']['full_bibliography'] = full_bib

            # Extract author: text before the book title in the bibliography
            book_title_text = result['book_title']
            author_str = _extract_text_before_em(bib_p)

            if author_str:
                author_str = author_str.strip().rstrip(',').strip()
                parsed = parse_author_string(author_str)
                result['book_author_first'] = parsed['first']
                result['book_author_last'] = parsed['last']
                result['_metadata']['is_edited_volume'] = parsed['is_edited']
                result['_metadata']['has_multiple_authors'] = parsed['has_multiple']
                if parsed['has_multiple']:
                    result['_metadata']['all_authors_raw'] = author_str

    # --- Reviewer ---
    reviewer_el = soup.select_one('p.meta-item__reviewer')
    if reviewer_el:
        # Try the meta tag first (clean structured data)
        meta_tag = reviewer_el.find('meta', property='name')
        if meta_tag and meta_tag.get('content'):
            reviewer_content = meta_tag['content']
        else:
            # Fallback: parse text, stripping "Reviewed by"
            reviewer_content = reviewer_el.get_text()
            reviewer_content = re.sub(r'^Reviewed?\s+by\s+', '', reviewer_content, flags=re.IGNORECASE)

        reviewer_parsed = parse_reviewer_string(reviewer_content)
        result['reviewer_first'] = reviewer_parsed['first']
        result['reviewer_last'] = reviewer_parsed['last']
        result['_metadata']['reviewer_affiliation'] = reviewer_parsed['affiliation']
        result['_metadata']['has_multiple_reviewers'] = reviewer_parsed['has_multiple']

    # --- Publication date ---
    # Primary: ISO 8601 from meta tag
    date_meta = soup.select_one('meta[property="datePublished"]')
    if date_meta and date_meta.get('content'):
        iso_date = date_meta['content']
        try:
            # Parse ISO 8601: "2023-12-29T13:29:00-05:00"
            dt = datetime.fromisoformat(iso_date)
            result['publication_date'] = dt.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            pass

    # Fallback: edition text (YYYY.MM.DD)
    if not result.get('publication_date'):
        edition_el = soup.select_one('p.meta-item__edition')
        if edition_el:
            edition_text = edition_el.get_text().strip()
            match = re.match(r'(\d{4})\.(\d{1,2})\.(\d{1,2})', edition_text)
            if match:
                year, month, day = match.groups()
                try:
                    month_int = int(month)
                    day_int = int(day)
                    if 1 <= month_int <= 12 and 1 <= day_int <= 31:
                        result['publication_date'] = f"{year}-{int(month):02d}-{int(day):02d}"
                except ValueError:
                    pass

    # --- Review summary (first substantial paragraph of the article body) ---
    article_body = soup.select_one('div.article-content')
    if article_body:
        for p in article_body.find_all('p'):
            text = _clean_text(p.get_text())
            if len(text) > 100:
                result['review_summary'] = text[:500] + ('...' if len(text) > 500 else '')
                break

    # --- DOI ---
    full_text = soup.get_text()
    doi_match = re.search(r'(?:doi|DOI):?\s*(10\.\d{4,}/[^\s,\n]+)', full_text)
    if doi_match:
        result['doi'] = doi_match.group(1).rstrip('.')

    return result


def parse_author_string(author_str: str) -> Dict:
    """
    Parse a bibliography author string into structured data.

    Handles:
      - "Thomas Nagel"
      - "A.W. Moore"
      - "Philip J. Ivanhoe and Hwa Yeong Wang"
      - "Uriah Kriegel (ed.)"
      - "John Smith and Jane Doe (eds.)"
      - "Jean-Paul Sartre"

    Returns dict with: first, last, is_edited, has_multiple
    """
    result = {'first': '', 'last': '', 'is_edited': False, 'has_multiple': False}

    if not author_str:
        return result

    # Detect and remove editor markers
    is_edited = bool(re.search(r'\(eds?\.?\)', author_str, re.IGNORECASE))
    cleaned = re.sub(r'\s*\(eds?\.?\)\s*', '', author_str, flags=re.IGNORECASE)
    cleaned = cleaned.strip().rstrip(',').strip()
    result['is_edited'] = is_edited

    # Detect multiple authors
    has_multiple = bool(re.search(r'\band\b', cleaned, re.IGNORECASE))
    result['has_multiple'] = has_multiple

    if has_multiple:
        # Take the first author (before "and")
        first_author = re.split(r'\s+and\s+', cleaned, maxsplit=1, flags=re.IGNORECASE)[0].strip()
        first_author = first_author.rstrip(',').strip()
    else:
        first_author = cleaned

    first, last = split_name(first_author)
    result['first'] = first
    result['last'] = last

    return result


def parse_reviewer_string(reviewer_str: str) -> Dict:
    """
    Parse a reviewer string (from the meta tag content) into structured data.

    Format is typically: "Name, Affiliation"
    e.g. "A.W. Moore, University of Oxford"
         "Timothy O'Connor and Nickolas Montgomery, Indiana University"

    Returns dict with: first, last, affiliation, has_multiple
    """
    result = {'first': '', 'last': '', 'affiliation': '', 'has_multiple': False}

    if not reviewer_str:
        return result

    reviewer_str = reviewer_str.strip()

    # Split name from affiliation on the last comma followed by something
    # that looks like an institution
    # But usually it's just the first comma: "Name, University of X"
    # We need to be careful: "Donald C. Ainslie, University of Toronto"
    # The periods after initials don't have commas, so the first comma is the separator.
    parts = reviewer_str.split(',', 1)
    name_part = parts[0].strip()
    affiliation = parts[1].strip() if len(parts) > 1 else ''
    result['affiliation'] = affiliation

    # Detect multiple reviewers
    has_multiple = bool(re.search(r'\band\b', name_part, re.IGNORECASE))
    result['has_multiple'] = has_multiple

    if has_multiple:
        # Take the first reviewer
        first_reviewer = re.split(r'\s+and\s+', name_part, maxsplit=1, flags=re.IGNORECASE)[0].strip()
    else:
        first_reviewer = name_part

    first, last = split_name(first_reviewer)
    result['first'] = first
    result['last'] = last

    return result


def split_name(name: str) -> Tuple[str, str]:
    """
    Split a full name into (first_names, last_name).

    Handles:
      - "Thomas Nagel" → ("Thomas", "Nagel")
      - "A.W. Moore" → ("A.W.", "Moore")
      - "Donald C. Ainslie" → ("Donald C.", "Ainslie")
      - "Frisbee C. C. Sheffield" → ("Frisbee C. C.", "Sheffield")
      - "Jean-Paul Sartre" → ("Jean-Paul", "Sartre")
      - "Philip J. Ivanhoe" → ("Philip J.", "Ivanhoe")

    Convention: last word is the last name, everything before is the first name.
    """
    if not name:
        return ('', '')

    name = name.strip()
    parts = name.split()

    if len(parts) == 0:
        return ('', '')
    elif len(parts) == 1:
        return ('', parts[0])
    else:
        return (' '.join(parts[:-1]), parts[-1])


def is_valid_review_url(url: str) -> bool:
    """Check if a URL looks like it could be a valid NDPR review URL."""
    if not url:
        return False

    if '/reviews/' not in url:
        return False

    # Not the main reviews page
    if url.rstrip('/').endswith('/reviews'):
        return False

    # Not an archive index page
    if '/archives/' in url:
        return False

    # Should have a meaningful slug
    slug = url.rstrip('/').split('/')[-1]
    if len(slug) < 3:
        return False

    # Avoid admin/meta pages
    exclude = ['admin', 'login', 'search', 'contact', 'about', 'rss', 'feed']
    if any(pattern == slug for pattern in exclude):
        return False

    return True


# --- Private helpers ---

def _extract_text_before_em(element) -> str:
    """
    Extract all text content that appears before the first <em> tag
    within an element, handling nested wrappers like <strong> and <span>.
    """
    texts = []
    _walk_before_em(element, texts)
    return ''.join(texts)


def _walk_before_em(element, texts: list) -> bool:
    """
    Recursively walk children, collecting text until we hit an <em> tag.
    Returns True if <em> was found (signals to stop).
    """
    for child in element.children:
        if hasattr(child, 'name') and child.name == 'em':
            return True  # Stop — we've reached the book title
        if isinstance(child, NavigableString):
            texts.append(str(child))
        elif hasattr(child, 'children'):
            # Recurse into inline wrappers like <strong>, <span>
            if _walk_before_em(child, texts):
                return True
    return False


def _clean_text(text: str) -> str:
    """Clean up extracted text: normalize whitespace, quotes, strip."""
    if not text:
        return ''
    # Replace &nbsp; and other whitespace
    text = text.replace('\xa0', ' ')
    # Normalize smart quotes/apostrophes to ASCII
    text = text.replace('\u2018', "'").replace('\u2019', "'")  # single quotes
    text = text.replace('\u201c', '"').replace('\u201d', '"')  # double quotes
    text = text.replace('\u2013', '-').replace('\u2014', '-')  # dashes
    text = re.sub(r'\s+', ' ', text)
    return text.strip()
