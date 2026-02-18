#!/usr/bin/env python3
"""
PhilReviews Multi-Journal Crossref Scraper

Scrapes book reviews from multiple philosophy journals via the Crossref API.
Auto-detects the title format used by each journal and applies the appropriate
parsing strategy.

Supported formats:
  A) Italic tags: "Author. <i>Title</i>. Publisher..." (Ethics, Utilitas, etc.)
  B) Italic title only: "<i>Title</i>" — book author looked up via OpenAlex (Phil Review)
  C) Title by Author: "Title by Author (review)" (Journal of the History of Philosophy)
  D) Generic "Book Review" title: enriched via Semantic Scholar API (Mind, Phil Quarterly)
  F) Title - Author: dash separator (Phil Quarterly older entries)
  E) Title, by Author: "Title, by Author" (Australasian J. Phil)
"""

import requests
import re
import time
import json
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from urllib.parse import quote, quote_plus
from dotenv import load_dotenv

import db

load_dotenv()


# --- Title format parsers ---

def _normalize(text: str) -> str:
    """Normalize whitespace, smart quotes, dashes."""
    text = text.replace('\xa0', ' ').replace('\u2002', ' ')
    text = text.replace('\u2018', "'").replace('\u2019', "'")
    text = text.replace('\u201c', '"').replace('\u201d', '"')
    text = text.replace('\u2010', '-').replace('\u2011', '-')
    text = text.replace('\u2013', '-').replace('\u2014', '-')
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def parse_review_title(title: str, subtitle: str = '', crossref_data: dict = None) -> Optional[Dict]:
    """
    Auto-detect the format of a Crossref book review title and parse it.

    Returns dict with: book_title, book_author_first, book_author_last,
                       is_edited_volume, has_multiple_authors,
                       needs_doi_scrape (bool)
    Or None if we can't parse it at all.
    """
    title = _normalize(title)
    subtitle = _normalize(subtitle) if subtitle else ''

    # Strip "Book Reviews" / "Book Review" prefix (Ethics, JMP format)
    stripped = re.sub(r'^Book\s*Reviews?\s*', '', title)

    # --- Format A/B: <i>/<em> tags present ---
    italic_match = re.search(r'<(?:i|em)>(.*?)</(?:i|em)>', stripped)
    if italic_match:
        book_title = re.sub(r'<[^>]+>', '', italic_match.group(1)).strip()
        pre_italic = stripped[:italic_match.start()]
        pre_italic = re.sub(r'<[^>]+>', '', pre_italic)
        pre_italic = re.sub(r'[,.\s]+$', '', pre_italic).strip()

        # Text AFTER the closing </i> tag — e.g. "<i>Title</i>. Author Name"
        post_italic = stripped[italic_match.end():]
        post_italic = re.sub(r'<[^>]+>', '', post_italic)  # strip stray HTML
        # Remove leading punctuation/whitespace: ". Author Name" → "Author Name"
        post_italic = re.sub(r'^[,.\s:;]+', '', post_italic).strip()
        # Remove publisher/city/year tail: "Author Name. New York: Publisher, 2005..."
        post_italic = re.split(r'\.\s+(?:[A-Z][a-z]+:|\d{4}|pp\.)', post_italic)[0].strip()
        post_italic = re.sub(r'[,.\s]+$', '', post_italic).strip()

        if not pre_italic and book_title:
            # No text before <i>, but check for author after </i>
            # Handle ", by Author. Edited by Editor" pattern (Mind format)
            by_match = re.match(r'^by\s+(.+)', post_italic, re.IGNORECASE)
            if by_match:
                author_part = by_match.group(1).strip()
                # Remove "Edited by ..." suffix
                author_part = re.split(r'\.\s*Edited\s+by\b', author_part, flags=re.IGNORECASE)[0].strip()
                author_part = re.sub(r'[,.\s]+$', '', author_part).strip()
                is_edited = bool(re.search(r'\bEdited\b', post_italic, re.IGNORECASE))
                first, last, has_multiple = _extract_first_author(author_part)
                if last:
                    return {
                        'book_title': book_title,
                        'book_author_first': first,
                        'book_author_last': last,
                        'is_edited_volume': is_edited,
                        'has_multiple_authors': has_multiple,
                        'needs_doi_scrape': False,
                        'format': 'italic_then_author',
                    }

            if post_italic and _looks_like_author_name(post_italic):
                is_edited = bool(re.search(r'\beds?\.?\b|\beditors?\b', post_italic, re.IGNORECASE))
                author_clean = re.sub(r',?\s*\beds?\.?\s*$|\beditors?\s*$', '',
                                      post_italic, flags=re.IGNORECASE).strip()
                first, last, has_multiple = _extract_first_author(author_clean)
                if last:
                    return {
                        'book_title': book_title,
                        'book_author_first': first,
                        'book_author_last': last,
                        'is_edited_volume': is_edited,
                        'has_multiple_authors': has_multiple,
                        'needs_doi_scrape': False,
                        'format': 'italic_then_author',
                    }

            # Format B: title is just <i>BookTitle</i> with no usable author
            return {
                'book_title': book_title,
                'book_author_first': '',
                'book_author_last': '',
                'is_edited_volume': False,
                'has_multiple_authors': False,
                'needs_doi_scrape': True,
                'format': 'italic_title_only',
            }

        # The text before <i> might be:
        #   (a) Just an author name: "Allhoff, Fritz" (Ethics format)
        #   (b) "Review of Author's" or "Review of Author," (Utilitas, Phil Science)
        #   (c) "Book symposium on Author," (Inquiry)
        #   (d) A review essay title with no author: "Critical reflections on" (EJP)

        # Try to extract author from "Review of / symposium on" patterns
        author_from_prefix = re.search(
            r'(?:review\s+of|symposium\s+on|book\s+symposium\s+on)\s+'
            r'([A-Z][a-zA-Z.\s-]+?)(?:[\'\']\s*s?\s*)?$',
            pre_italic, re.IGNORECASE
        )

        if author_from_prefix:
            author_str = author_from_prefix.group(1).strip().rstrip(',').strip()
        else:
            # Assume the whole pre_italic section is the author (Ethics format)
            author_str = pre_italic

        is_edited = bool(re.search(r'\beds?\.?\b|\beditors?\b', author_str, re.IGNORECASE))
        author_clean = re.sub(r',?\s*\beds?\.?\s*$|\beditors?\s*$', '', author_str,
                              flags=re.IGNORECASE).strip().rstrip(',').strip()

        first, last, has_multiple = _extract_first_author(author_clean)

        # Validate: if the "author" looks like a title fragment (lowercase words,
        # too long, contains certain keywords), mark as needing DOI scrape instead
        if last and _looks_like_author_name(author_clean):
            if book_title:
                return {
                    'book_title': book_title,
                    'book_author_first': first,
                    'book_author_last': last,
                    'is_edited_volume': is_edited,
                    'has_multiple_authors': has_multiple,
                    'needs_doi_scrape': False,
                    'format': 'italic_tags',
                }

        # Pre-italic didn't yield an author — try post-italic as fallback
        if book_title and post_italic and _looks_like_author_name(post_italic):
            is_edited_post = bool(re.search(r'\beds?\.?\b|\beditors?\b', post_italic, re.IGNORECASE))
            author_clean_post = re.sub(r',?\s*\beds?\.?\s*$|\beditors?\s*$', '',
                                       post_italic, flags=re.IGNORECASE).strip()
            first_post, last_post, has_multiple_post = _extract_first_author(author_clean_post)
            if last_post:
                return {
                    'book_title': book_title,
                    'book_author_first': first_post,
                    'book_author_last': last_post,
                    'is_edited_volume': is_edited_post,
                    'has_multiple_authors': has_multiple_post,
                    'needs_doi_scrape': False,
                    'format': 'italic_then_author',
                }

        # We have a book title but couldn't reliably get the author
        if book_title:
            return {
                'book_title': book_title,
                'book_author_first': '',
                'book_author_last': '',
                'is_edited_volume': False,
                'has_multiple_authors': False,
                'needs_doi_scrape': True,
                'format': 'italic_title_only',
            }

    # --- Format C: "Title by Author (review)" (JHP style) ---
    jhp_match = re.match(r'^(.+?)\s+by\s+(.+?)\s*\(review\)\s*$', stripped, re.IGNORECASE)
    if jhp_match:
        book_title = jhp_match.group(1).strip()
        author_str = jhp_match.group(2).strip()
        is_edited = bool(re.search(r'\beds?\.?\b|\beditors?\b', author_str, re.IGNORECASE))
        author_clean = re.sub(r',?\s*\beds?\.?\s*$', '', author_str,
                              flags=re.IGNORECASE).strip().rstrip(',').strip()
        first, last, has_multiple = _extract_first_author(author_clean)
        if book_title and last:
            return {
                'book_title': book_title,
                'book_author_first': first,
                'book_author_last': last,
                'is_edited_volume': is_edited,
                'has_multiple_authors': has_multiple,
                'needs_doi_scrape': False,
                'format': 'title_by_author',
            }

    # --- Format E: "Title, by Author" or "A Review of Title, by Author" (AJP style) ---
    ajp_match = re.match(
        r'^(?:A\s+Review\s+of\s+["\u201c]?)?(.+?)["\u201d]?,\s+by\s+(.+?)$',
        stripped, re.IGNORECASE
    )
    if ajp_match:
        book_title = ajp_match.group(1).strip().strip('"').strip()
        author_str = ajp_match.group(2).strip()
        is_edited = bool(re.search(r'\beds?\.?\b|\beditors?\b', author_str, re.IGNORECASE))
        author_clean = re.sub(r',?\s*\beds?\.?\s*$', '', author_str,
                              flags=re.IGNORECASE).strip().rstrip(',').strip()
        first, last, has_multiple = _extract_first_author(author_clean)
        if book_title and last:
            return {
                'book_title': book_title,
                'book_author_first': first,
                'book_author_last': last,
                'is_edited_volume': is_edited,
                'has_multiple_authors': has_multiple,
                'needs_doi_scrape': False,
                'format': 'title_by_author',
            }

    # --- Format G: "Review: Author: Title" (Mind mid-era format) ---
    review_colon_match = re.match(r'^Review:\s*(.+?):\s+(.+)$', stripped)
    if review_colon_match:
        author_str = review_colon_match.group(1).strip()
        book_title = review_colon_match.group(2).strip()
        if _looks_like_author_name(author_str) and len(book_title) > 3:
            is_edited = bool(re.search(r'\beds?\.?\b|\beditors?\b', author_str, re.IGNORECASE))
            author_clean = re.sub(r',?\s*\beds?\.?\s*$', '', author_str,
                                  flags=re.IGNORECASE).strip()
            first, last, has_multiple = _extract_first_author(author_clean)
            if last:
                return {
                    'book_title': book_title,
                    'book_author_first': first,
                    'book_author_last': last,
                    'is_edited_volume': is_edited,
                    'has_multiple_authors': has_multiple,
                    'needs_doi_scrape': False,
                    'format': 'review_colon_author_title',
                }

    # --- Format F: "Title - Author" or "Title- Author (eds)" (Phil Quarterly old format) ---
    dash_match = re.match(r'^(.+?)\s*[-\u2013\u2014]\s*(.+?)$', stripped)
    if dash_match:
        book_title = dash_match.group(1).strip()
        author_str = dash_match.group(2).strip()
        # Validate: title should be >3 chars, author should look like a name
        is_edited = bool(re.search(r'\beds?\.?\b|\beditors?\b', author_str, re.IGNORECASE))
        author_clean = re.sub(r',?\s*\(eds?\.\)\s*$|\beds?\.?\s*$|\beditors?\s*$', '',
                              author_str, flags=re.IGNORECASE).strip().rstrip(',').strip()
        first, last, has_multiple = _extract_first_author(author_clean)
        if book_title and last and len(book_title) > 3 and _looks_like_author_name(author_clean):
            return {
                'book_title': book_title,
                'book_author_first': first,
                'book_author_last': last,
                'is_edited_volume': is_edited,
                'has_multiple_authors': has_multiple,
                'needs_doi_scrape': False,
                'format': 'title_dash_author',
            }

    # --- Format D: title is "Book Review" or similar generic text ---
    if stripped.strip().lower() in ('book review', 'book reviews', 'book review.',
                                    'book received', 'book notes', 'book note'):
        return {
            'book_title': '',
            'book_author_first': '',
            'book_author_last': '',
            'is_edited_volume': False,
            'has_multiple_authors': False,
            'needs_doi_scrape': True,
            'format': 'generic_title',
        }

    # --- Fallback: try plain text "LastName, First. Title. Publisher..." ---
    plain = re.sub(r'<[^>]+>', '', stripped).strip()
    plain = re.sub(r',\s*,+', ',', plain)
    fallback = re.match(r'^([A-Z][^.]+?)\.\s+([^.]+?)\.', plain)
    if fallback:
        author_section = fallback.group(1).strip()
        book_title = fallback.group(2).strip()
        is_edited = bool(re.search(r'\beds?\.?\b', author_section, re.IGNORECASE))
        author_clean = re.sub(r',?\s*\beds?\.?\s*$', '', author_section,
                              flags=re.IGNORECASE).strip().rstrip(',').strip()
        first, last, has_multiple = _extract_first_author(author_clean)
        if book_title and last and len(book_title) > 3:
            return {
                'book_title': book_title,
                'book_author_first': first,
                'book_author_last': last,
                'is_edited_volume': is_edited,
                'has_multiple_authors': has_multiple,
                'needs_doi_scrape': False,
                'format': 'fallback',
            }

    return None


def _looks_like_author_name(text: str) -> bool:
    """
    Heuristic: does this text look like a person's name rather than a title fragment?
    Used to validate that text extracted before <i> tags is actually an author.
    """
    if not text:
        return False

    words = text.split()
    if len(words) < 1 or len(words) > 6:
        return False

    # Author names are short and mostly capitalized words
    # Title fragments tend to have lowercase words like "reflections", "symposium"
    non_name_words = {'the', 'a', 'an', 'of', 'on', 'in', 'for', 'and', 'to', 'from',
                      'review', 'book', 'symposium', 'critical', 'reflections',
                      'commentary', 'response', 'reply', 'essay', 'matter', 'body',
                      'special', 'is', 'what', 'how', 'why', 'case', 'against',
                      'beyond', 'toward', 'towards', 'between'}

    # If most words are non-name words, this is probably a title fragment
    lower_words = [w.lower().rstrip('.,;:?!') for w in words]
    non_name_count = sum(1 for w in lower_words if w in non_name_words)
    if non_name_count > len(words) / 2:
        return False

    # Check that at least the last word starts with uppercase (last name)
    last_word = words[-1]
    if not last_word[0].isupper() and not last_word[0] == "'":
        return False

    return True


def _extract_first_author(author_str: str) -> Tuple[str, str, bool]:
    """
    Extract the first author's (first, last) from an author string.
    Handles "Last, First", "First Last", "A and B", initials, Jr/Sr.
    Returns (first, last, has_multiple).
    """
    if not author_str:
        return ('', '', False)

    # Clean up
    author_str = author_str.replace(';', ',')
    author_str = re.sub(r',?\s*(eds?\.?|trans\.?|translator|editor)(\s|$)', '',
                        author_str, flags=re.IGNORECASE)
    author_str = re.sub(r'[,.\s]+$', '', author_str).strip()

    comma_count = author_str.count(',')
    has_jr_sr = bool(re.search(r',\s*(Jr|Sr)\.?', author_str, flags=re.IGNORECASE))
    effective_commas = comma_count - (1 if has_jr_sr else 0)
    has_multiple = ' and ' in author_str.lower() or effective_commas > 1

    if has_multiple:
        # Take the part before "and" or first comma-separated chunk
        first_chunk = re.split(r'\s+and\s+', author_str, maxsplit=1, flags=re.IGNORECASE)[0]
        first_chunk = first_chunk.strip().rstrip(',').strip()
        # If "Last, First" format
        if ',' in first_chunk:
            parts = first_chunk.split(',', 1)
            return (parts[1].strip(), parts[0].strip(), True)
        # "First Last" format
        parts = first_chunk.split()
        if len(parts) >= 2:
            return (' '.join(parts[:-1]), parts[-1], True)
        elif len(parts) == 1:
            return ('', parts[0], True)
        return ('', '', True)

    # Single author
    if ',' in author_str:
        parts = author_str.split(',')
        last = parts[0].strip()
        first = parts[1].strip() if len(parts) >= 2 else ''
        # Handle Jr/Sr
        if len(parts) >= 3:
            jr_sr = parts[2].strip()
            if re.match(r'(Jr|Sr)\.?$', jr_sr, flags=re.IGNORECASE):
                last = f"{last}, {jr_sr}"
        return (first, last, False)

    # No comma: "First Last"
    parts = author_str.split()
    if len(parts) >= 2:
        return (' '.join(parts[:-1]), parts[-1], False)
    elif len(parts) == 1:
        return ('', parts[0], False)
    return ('', '', False)


# --- Book review detection ---

def is_book_review(crossref_item: dict) -> bool:
    """Check if a Crossref work item is a book review."""
    title = (crossref_item.get('title', ['']) or [''])[0].lower()

    # Exclude non-review items
    exclude = ['editorial:', 'announcing', 'comment on', 'response to', 'reply to',
               'correction', 'erratum', 'retraction', 'call for papers',
               'book received', 'book notes', 'books received']
    for pattern in exclude:
        if pattern in title:
            return False

    # Positive indicators
    # Italic tags suggest a book title, but only if the italic text is substantial
    # (short italic fragments are likely emphasis, variables, or foreign words)
    italic_match = re.search(r'<(?:i|em)>(.*?)</(?:i|em)>', title)
    if italic_match:
        italic_text = re.sub(r'<[^>]+>', '', italic_match.group(1)).strip()
        if len(italic_text) >= 15:
            return True
    if '(review)' in title:
        return True

    indicators = ['book review', 'book reviews', 'review of', 'reviewed work']
    for ind in indicators:
        if ind in title:
            return True

    # "Review: Author: Title" (Mind format)
    if re.match(r'^review:\s', title):
        return True

    # Pattern: starts with "Author, First. Title" (common Crossref book review format)
    raw_title = (crossref_item.get('title', ['']) or [''])[0]
    if re.search(r'^[A-Z][^,]+,\s+[A-Z]', raw_title):
        return True

    # Pattern: "Title, by Author"
    if re.search(r',\s+by\s+[A-Z]', raw_title):
        return True

    return False


# --- Main scraper class ---

class CrossrefReviewScraper:
    """Scrapes book reviews from multiple philosophy journals via the Crossref API."""

    # Journals and their known Crossref title formats.
    # 'crossref_parseable': book title + author extractable from Crossref title alone
    # 'openalex_enrichable': book title in Crossref, author looked up via OpenAlex
    # 'skip': needs headless browser (Cloudflare-protected), not yet supported
    JOURNALS = {
        # Category A: <i> tags with author before them
        'Ethics': {'crossref_parseable': True},
        'Utilitas': {'crossref_parseable': True},
        'Inquiry': {'crossref_parseable': True},
        'Philosophy of Science': {'crossref_parseable': True},
        'European Journal of Philosophy': {'crossref_parseable': True},
        # Category C: "Title by Author (review)"
        'Journal of the History of Philosophy': {'crossref_parseable': True},
        # Category E: "Title, by Author"
        'Australasian Journal of Philosophy': {'crossref_parseable': True},
        # Category B: <i>Title</i> only — book author from OpenAlex
        'The Philosophical Review': {'crossref_parseable': False, 'openalex_enrichable': True},
        # Category D: generic "Book Review" — enriched via Semantic Scholar
        'Mind': {'crossref_parseable': False, 'semantic_scholar_enrichable': True},
        # Category F/D mix: older entries have "Title - Author", newer are generic
        'The Philosophical Quarterly': {'crossref_parseable': False, 'semantic_scholar_enrichable': True},
    }

    def __init__(self):
        self.crossref_email = os.getenv('CROSSREF_EMAIL', 'user@example.com')

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': f'PhilReviews/2.0 (mailto:{self.crossref_email})'
        })

        self.stats = {
            'journals_searched': 0,
            'dois_found': 0,
            'parsed_from_crossref': 0,
            'openalex_found': 0,
            'semantic_scholar_found': 0,
            'uploaded': 0,
            'duplicates_skipped': 0,
            'errors': 0,
        }
        self.results = []

    def log(self, msg: str, level: str = "INFO"):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] {level}: {msg}")

    # --- Crossref API ---

    def search_journal(self, journal_name: str, max_results: int = 0) -> List[dict]:
        """Fetch all articles from a journal via Crossref and filter to book reviews.

        Uses cursor-based pagination (no offset limit) and no text query so that
        reviews whose titles lack the words "book review" are not missed.

        Args:
            journal_name: Crossref container-title to filter on.
            max_results: Stop after this many *total* items fetched (0 = no limit).
        """
        self.log(f"Searching {journal_name}...")
        all_items = []
        cursor = '*'
        page = 0

        while True:
            try:
                params = {
                    'filter': f'container-title:{journal_name}',
                    'rows': 100,
                    'cursor': cursor,
                    'mailto': self.crossref_email,
                }
                resp = self.session.get(
                    'https://api.crossref.org/works', params=params, timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                items = data.get('message', {}).get('items', [])
                if not items:
                    break
                all_items.extend(items)
                page += 1

                if page % 10 == 0:
                    self.log(f"  {journal_name}: fetched {len(all_items)} items so far...")

                if max_results and len(all_items) >= max_results:
                    all_items = all_items[:max_results]
                    break

                cursor = data.get('message', {}).get('next-cursor', '')
                if not cursor:
                    break

                time.sleep(0.5)
            except Exception as e:
                self.log(f"Error fetching from Crossref: {e}", "ERROR")
                self.stats['errors'] += 1
                break

        # Filter to book reviews client-side
        reviews = [item for item in all_items if is_book_review(item)]
        self.log(f"  Found {len(all_items)} items, {len(reviews)} book reviews")
        self.stats['journals_searched'] += 1
        self.stats['dois_found'] += len(reviews)
        return reviews

    # --- Review data extraction ---

    def extract_review(self, crossref_item: dict) -> Optional[Dict]:
        """Extract review data from a Crossref item. Returns dict for DB insertion."""
        title = (crossref_item.get('title', ['']) or [''])[0]
        subtitle = (crossref_item.get('subtitle', ['']) or [''])[0] if crossref_item.get('subtitle') else ''
        doi = crossref_item.get('DOI', '')
        container = (crossref_item.get('container-title', ['']) or [''])[0]

        # Get reviewer from Crossref author field
        reviewer_first = ''
        reviewer_last = ''
        authors = crossref_item.get('author', [])
        if authors:
            reviewer_first = authors[0].get('given', '')
            reviewer_last = authors[0].get('family', '')

        # Get publication date
        pub_date = ''
        issued = crossref_item.get('issued', {})
        if issued.get('date-parts'):
            parts = issued['date-parts'][0]
            year = parts[0] if len(parts) >= 1 else 0
            month = parts[1] if len(parts) > 1 else 1
            day = parts[2] if len(parts) > 2 else 1
            if year:
                pub_date = f"{year:04d}-{month:02d}-{day:02d}"

        # Get review link
        review_link = crossref_item.get('URL', '')
        if review_link and not review_link.startswith('http'):
            review_link = 'https://' + review_link

        # Get abstract
        abstract = crossref_item.get('abstract', '')
        if abstract:
            abstract = re.sub(r'<[^>]+>', '', abstract).strip()

        # Access type
        access_type = 'Open' if crossref_item.get('license') else 'Restricted'

        # Parse the title to get book info
        parsed = parse_review_title(title, subtitle, crossref_item)

        if not parsed:
            return None

        record = {
            'Book Title': _normalize(parsed['book_title']) if parsed['book_title'] else '',
            'Book Author First Name': _normalize(parsed['book_author_first']),
            'Book Author Last Name': _normalize(parsed['book_author_last']),
            'Reviewer First Name': _normalize(reviewer_first),
            'Reviewer Last Name': _normalize(reviewer_last),
            'Publication Source': container,
            'Publication Date': pub_date,
            'Review Link': review_link,
            'Review Summary': abstract[:500] + ('...' if len(abstract) > 500 else '') if abstract else '',
            'Access Type': access_type,
            'DOI': doi,
        }

        # Track if we need DOI scraping
        if parsed.get('needs_doi_scrape'):
            record['_needs_doi_scrape'] = True
            record['_format'] = parsed.get('format', '')

        # Filter: if the book author and reviewer are the same person,
        # this is likely a symposium piece or research article, not a review
        if (record.get('Book Author Last Name') and record.get('Reviewer Last Name')
                and record['Book Author Last Name'].lower() == record['Reviewer Last Name'].lower()
                and record['Book Author First Name'].lower() == record['Reviewer First Name'].lower()):
            return None

        self.stats['parsed_from_crossref'] += 1
        return record

    # --- OpenAlex book author lookup (Category B enrichment) ---

    def _normalize_for_comparison(self, title: str) -> str:
        """Normalize a title for fuzzy comparison."""
        t = re.sub(r'<[^>]+>', '', title)
        t = t.split(':')[0]  # drop subtitle
        t = re.sub(r'[^a-z0-9 ]', '', t.lower())
        return t.strip()

    def _titles_match(self, book_title: str, openalex_title: str) -> bool:
        """Check if two book titles are a reasonable match."""
        norm_book = self._normalize_for_comparison(book_title)
        norm_oa = self._normalize_for_comparison(openalex_title)
        if not norm_book or not norm_oa:
            return False
        if norm_book.startswith(norm_oa) or norm_oa.startswith(norm_book):
            return True
        book_words = set(norm_book.split())
        oa_words = set(norm_oa.split())
        if len(book_words) == 0:
            return False
        overlap = len(book_words & oa_words) / max(len(book_words), len(oa_words))
        return overlap > 0.7

    def lookup_book_author(self, book_title: str) -> Optional[Tuple[str, str]]:
        """
        Look up the author of a book via OpenAlex API.
        Returns (first_name, last_name) or None if not found.
        """
        if not book_title or len(book_title) < 4:
            return None

        # Use the main title (before colon) for better search results
        search_title = book_title.split(':')[0].strip()
        try:
            resp = self.session.get(
                'https://api.openalex.org/works',
                params={
                    'search': search_title,
                    'select': 'id,title,authorships,publication_year',
                    'per_page': 5,
                    'mailto': self.crossref_email,
                },
                timeout=15,
            )
            if resp.status_code != 200:
                return None

            results = resp.json().get('results', [])
            for result in results:
                oa_title = result.get('title', '')
                if self._titles_match(book_title, oa_title):
                    authorships = result.get('authorships', [])
                    if authorships:
                        author = authorships[0].get('author', {})
                        display_name = author.get('display_name', '')
                        if display_name:
                            parts = display_name.split()
                            if len(parts) >= 2:
                                return (' '.join(parts[:-1]), parts[-1])
                            elif len(parts) == 1:
                                return ('', parts[0])
            return None
        except Exception as e:
            self.log(f"  OpenAlex lookup error for '{search_title}': {e}", "WARNING")
            return None

    def enrich_with_openalex(self, records: List[Dict]) -> None:
        """
        Enrich records that have a book title but no author via OpenAlex.
        Modifies records in place.
        """
        needs_author = [r for r in records
                        if r.get('Book Title')
                        and not r.get('Book Author Last Name')]

        if not needs_author:
            return

        self.log(f"Looking up {len(needs_author)} book authors via OpenAlex...")
        found = 0
        for i, record in enumerate(needs_author):
            if i > 0 and i % 20 == 0:
                self.log(f"  OpenAlex progress: {i}/{len(needs_author)} ({found} found)")

            author = self.lookup_book_author(record['Book Title'])
            if author:
                record['Book Author First Name'] = author[0]
                record['Book Author Last Name'] = author[1]
                found += 1

            time.sleep(0.2)  # Rate limit

        self.log(f"  OpenAlex enrichment: {found}/{len(needs_author)} authors found")
        self.stats['openalex_found'] = found

    # --- Semantic Scholar enrichment (Category D: generic "Book Review" titles) ---

    def _parse_s2_title(self, s2_title: str) -> Optional[Dict]:
        """
        Parse a Semantic Scholar title that contains book info.

        Handles:
          - "Book Title, by Author Name"  (newer Mind format)
          - "Book Review. Book Title Author Name" (older Mind format)
          - "Book Title - Author Name" (Phil Quarterly)
        """
        if not s2_title or s2_title.lower().strip() in ('book review', 'book reviews'):
            return None

        s2_title = _normalize(s2_title)

        # Pattern 1: "Title, by Author" or "Title, by Author."
        m = re.match(r'^(.+?),\s+by\s+(.+?)\.?\s*$', s2_title, re.IGNORECASE)
        if m:
            book_title = m.group(1).strip()
            author_str = m.group(2).strip().rstrip('.')
            first, last, has_multiple = _extract_first_author(author_str)
            if book_title and last:
                return {
                    'book_title': book_title,
                    'book_author_first': first,
                    'book_author_last': last,
                    'has_multiple_authors': has_multiple,
                }

        # Pattern 2: "Book Review. Title Author" (older Mind)
        m = re.match(r'^Book\s+Reviews?\.\s*(.+)$', s2_title, re.IGNORECASE)
        if m:
            remainder = m.group(1).strip()
            # The last 1-3 capitalized words are the author name
            # e.g. "Artworks Robert Stecker" → title="Artworks", author="Robert Stecker"
            # "The Nature of Perception John Foster" → title="The Nature of Perception", author="John Foster"
            words = remainder.split()
            # Try taking last 2 or 3 words as author
            for author_word_count in [3, 2]:
                if len(words) > author_word_count:
                    potential_author = ' '.join(words[-author_word_count:])
                    potential_title = ' '.join(words[:-author_word_count])
                    if _looks_like_author_name(potential_author) and len(potential_title) > 3:
                        first, last, has_multiple = _extract_first_author(potential_author)
                        if last:
                            return {
                                'book_title': potential_title,
                                'book_author_first': first,
                                'book_author_last': last,
                                'has_multiple_authors': has_multiple,
                            }

        # Pattern 3: "Title. Author Name" (from Crossref-style titles in S2)
        m = re.match(r'^(.+?)\.\s+([A-Z][a-zA-Z.\s-]+?)\.?\s*$', s2_title)
        if m:
            book_title = m.group(1).strip()
            author_str = m.group(2).strip().rstrip('.')
            if _looks_like_author_name(author_str) and len(book_title) > 3:
                first, last, has_multiple = _extract_first_author(author_str)
                if last:
                    return {
                        'book_title': book_title,
                        'book_author_first': first,
                        'book_author_last': last,
                        'has_multiple_authors': has_multiple,
                    }

        # Pattern 4: "Title - Author" or "Title – Author"
        m = re.match(r'^(.+?)\s*[-\u2013\u2014]\s*(.+?)$', s2_title)
        if m:
            book_title = m.group(1).strip()
            author_str = m.group(2).strip()
            if _looks_like_author_name(author_str):
                first, last, has_multiple = _extract_first_author(author_str)
                if book_title and last and len(book_title) > 3:
                    return {
                        'book_title': book_title,
                        'book_author_first': first,
                        'book_author_last': last,
                        'has_multiple_authors': has_multiple,
                    }

        return None

    def enrich_with_semantic_scholar(self, records: List[Dict]) -> None:
        """
        Enrich records still missing book title or author via Semantic Scholar.
        S2 often has the actual "Title, by Author" format that Crossref lacks.
        Modifies records in place.
        """
        needs_enrichment = [r for r in records
                            if r.get('DOI')
                            and (not r.get('Book Title') or not r.get('Book Author Last Name'))]

        if not needs_enrichment:
            return

        self.log(f"Looking up {len(needs_enrichment)} reviews via Semantic Scholar...")
        found = 0

        # Process in batches of 20 (smaller batches = fewer 400 errors from bad DOIs)
        for batch_start in range(0, len(needs_enrichment), 20):
            batch = needs_enrichment[batch_start:batch_start + 20]
            # Sanitize DOIs: only include well-formed ones
            valid_pairs = [(r, r.get('DOI', '')) for r in batch
                           if r.get('DOI') and '/' in r.get('DOI', '')]

            if not valid_pairs:
                continue

            try:
                resp = self.session.post(
                    'https://api.semanticscholar.org/graph/v1/paper/batch',
                    params={'fields': 'title,authors,externalIds'},
                    json={'ids': [f'DOI:{doi}' for _, doi in valid_pairs]},
                    timeout=30,
                )
                if resp.status_code != 200:
                    self.log(f"  S2 batch error: {resp.status_code}", "WARNING")
                    # Fall back to individual lookups for this batch
                    for record, doi in valid_pairs:
                        try:
                            r2 = self.session.get(
                                f'https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}',
                                params={'fields': 'title,authors'},
                                timeout=15,
                            )
                            if r2.status_code == 200:
                                s2_title = r2.json().get('title', '')
                                parsed = self._parse_s2_title(s2_title)
                                if parsed and parsed.get('book_author_last'):
                                    if not record.get('Book Title') and parsed.get('book_title'):
                                        record['Book Title'] = parsed['book_title']
                                    record['Book Author First Name'] = parsed['book_author_first']
                                    record['Book Author Last Name'] = parsed['book_author_last']
                                    found += 1
                            time.sleep(1.0)
                        except Exception:
                            continue
                    continue

                s2_results = resp.json()

                for (record, doi), s2_result in zip(valid_pairs, s2_results):
                    if s2_result is None:
                        continue

                    s2_title = s2_result.get('title', '')
                    parsed = self._parse_s2_title(s2_title)
                    if parsed and parsed.get('book_author_last'):
                        if not record.get('Book Title') and parsed.get('book_title'):
                            record['Book Title'] = parsed['book_title']
                        record['Book Author First Name'] = parsed['book_author_first']
                        record['Book Author Last Name'] = parsed['book_author_last']
                        found += 1

                time.sleep(1.0)  # S2 rate limit: ~1 req/sec for unauthenticated

            except Exception as e:
                self.log(f"  S2 batch error: {e}", "WARNING")

        self.log(f"  Semantic Scholar enrichment: {found}/{len(needs_enrichment)} reviews enriched")
        self.stats['semantic_scholar_found'] = found

    # --- Database operations ---

    def upload_to_db(self, records: List[Dict]) -> int:
        """Insert records into the local SQLite database, skipping duplicates by DOI."""
        if not records:
            return 0

        new_records = []
        for record in records:
            doi = record.get('DOI', '')
            if doi and db.doi_exists(doi):
                self.stats['duplicates_skipped'] += 1
                continue
            # Remove internal metadata keys
            clean = {k: v for k, v in record.items() if not k.startswith('_') and v}
            new_records.append(clean)

        if not new_records:
            return 0

        db_records = [_to_db_fields(r) for r in new_records]
        db.insert_reviews(db_records)
        self.stats['uploaded'] += len(db_records)
        return len(db_records)

    # --- Main pipeline ---

    def run(self, journals: List[str] = None, max_per_journal: int = 0,
            dry_run: bool = False, skip_enrichment: bool = False):
        """
        Run the scraper across multiple journals.

        Args:
            journals: List of journal names to search. Defaults to all configured journals.
            max_per_journal: Max items to fetch per journal (0 = all).
            dry_run: If True, don't insert into database.
            skip_enrichment: If True, skip OpenAlex and Semantic Scholar lookups.
        """
        start = datetime.now()

        if journals is None:
            journals = list(self.JOURNALS.keys())

        self.log(f"Starting multi-journal scraper for {len(journals)} journals")
        all_records = []

        for journal in journals:
            items = self.search_journal(journal, max_results=max_per_journal)

            journal_records = []
            for item in items:
                record = self.extract_review(item)
                if record:
                    journal_records.append(record)

            self.log(f"  Extracted {len(journal_records)} records from {journal}")
            all_records.extend(journal_records)

        if not skip_enrichment:
            # OpenAlex: look up book authors for Category B (have title, need author)
            self.enrich_with_openalex(all_records)
            # Semantic Scholar: look up everything for Category D (generic "Book Review")
            self.enrich_with_semantic_scholar(all_records)

        # Print results summary
        self._print_results(all_records)

        # Upload
        if not dry_run:
            # Only upload records that have at least a book title
            uploadable = [r for r in all_records if r.get('Book Title')]
            self.log(f"Inserting {len(uploadable)} records into database...")
            self.upload_to_db(uploadable)
        else:
            self.log("Dry run — skipping database insert")

        # Final stats
        duration = datetime.now() - start
        self.log(f"\nCompleted in {str(duration).split('.')[0]}")
        self.log(f"Stats: {json.dumps(self.stats, indent=2)}")

        self.results = all_records
        return all_records

    def _print_results(self, records: List[Dict]):
        """Print a summary of extracted records grouped by journal."""
        by_journal = {}
        for r in records:
            j = r.get('Publication Source', 'Unknown')
            by_journal.setdefault(j, []).append(r)

        print()
        print("=" * 70)
        print("EXTRACTION RESULTS")
        print("=" * 70)

        for journal, recs in sorted(by_journal.items()):
            has_title = sum(1 for r in recs if r.get('Book Title'))
            has_author = sum(1 for r in recs if r.get('Book Author Last Name'))
            needs_scrape = sum(1 for r in recs if r.get('_needs_doi_scrape'))
            print(f"\n{journal}: {len(recs)} reviews")
            print(f"  Book title extracted: {has_title}/{len(recs)}")
            print(f"  Book author extracted: {has_author}/{len(recs)}")
            if needs_scrape:
                print(f"  Needed DOI scrape: {needs_scrape}")

            # Show first 3 as examples
            for r in recs[:3]:
                title = r.get('Book Title', '?')[:50]
                author = f"{r.get('Book Author First Name', '')} {r.get('Book Author Last Name', '')}".strip() or '?'
                reviewer = f"{r.get('Reviewer First Name', '')} {r.get('Reviewer Last Name', '')}".strip() or '?'
                print(f"    - {title} | by {author} | reviewed by {reviewer}")

        print(f"\n{'=' * 70}")
        total = len(records)
        with_title = sum(1 for r in records if r.get('Book Title'))
        with_author = sum(1 for r in records if r.get('Book Author Last Name'))
        print(f"TOTAL: {total} reviews, {with_title} with titles ({with_title/total*100:.0f}%), "
              f"{with_author} with authors ({with_author/total*100:.0f}%)")


def _to_db_fields(record: dict) -> dict:
    """Convert Airtable-style field names to snake_case DB columns."""
    return {
        'book_title': record.get('Book Title', ''),
        'book_author_first_name': record.get('Book Author First Name', ''),
        'book_author_last_name': record.get('Book Author Last Name', ''),
        'reviewer_first_name': record.get('Reviewer First Name', ''),
        'reviewer_last_name': record.get('Reviewer Last Name', ''),
        'publication_source': record.get('Publication Source', ''),
        'publication_date': record.get('Publication Date', ''),
        'review_link': record.get('Review Link', ''),
        'review_summary': record.get('Review Summary', ''),
        'access_type': record.get('Access Type', ''),
        'doi': record.get('DOI', ''),
    }


def main():
    import argparse

    parser = argparse.ArgumentParser(description='PhilReviews Multi-Journal Crossref Scraper')
    parser.add_argument('--journals', nargs='+',
                        help='Specific journals to search (default: all)')
    parser.add_argument('--max-per-journal', type=int, default=0,
                        help='Max items to fetch per journal (default: 0 = all)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Don\'t insert into database')
    parser.add_argument('--skip-enrichment', action='store_true',
                        help='Skip OpenAlex and Semantic Scholar lookups')
    parser.add_argument('--list-journals', action='store_true',
                        help='List configured journals and exit')

    args = parser.parse_args()

    if args.list_journals:
        print("Configured journals:")
        for j, info in CrossrefReviewScraper.JOURNALS.items():
            if info['crossref_parseable']:
                status = "Crossref"
            elif info.get('openalex_enrichable'):
                status = "Crossref + OpenAlex"
            elif info.get('semantic_scholar_enrichable'):
                status = "Crossref + Semantic Scholar"
            else:
                status = "unknown"
            print(f"  - {j} ({status})")
        return

    scraper = CrossrefReviewScraper()
    scraper.run(
        journals=args.journals,
        max_per_journal=args.max_per_journal,
        dry_run=args.dry_run,
        skip_enrichment=args.skip_enrichment,
    )


if __name__ == '__main__':
    main()
