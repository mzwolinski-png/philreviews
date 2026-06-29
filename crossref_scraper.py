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
from scraper_base import BaseScraper
from journals import JOURNALS as _JOURNALS_CONFIG

load_dotenv()


from crossref_parsing import (
    _normalize, parse_review_title, _looks_like_author_name,
    _extract_first_author, is_book_review,
)



# --- Main scraper class ---

class CrossrefReviewScraper(BaseScraper):
    """Scrapes book reviews from multiple philosophy journals via the Crossref API."""

    # Journals and their known Crossref title formats.
    # 'crossref_parseable': book title + author extractable from Crossref title alone
    # 'openalex_enrichable': book title in Crossref, author looked up via OpenAlex
    # 'skip': needs headless browser (Cloudflare-protected), not yet supported
    JOURNALS = _JOURNALS_CONFIG

    name = "crossref"

    def __init__(self):
        super().__init__()
        self.crossref_email = os.getenv('CROSSREF_EMAIL', 'user@example.com')
        # Override User-Agent with Crossref polite-pool email
        self.session.headers.update({
            'User-Agent': f'PhilReviews/2.0 (mailto:{self.crossref_email})'
        })
        # Keep self.log callable for backward compatibility with self.log("msg")
        # while also supporting self.log.info() etc. via the _logger attribute
        self._logger = self.log
        self.log = self._compat_log

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

    def _compat_log(self, msg: str, level: str = "INFO"):
        """Compatibility wrapper: routes self.log("msg", "LEVEL") to the logger."""
        getattr(self._logger, level.lower(), self._logger.info)(msg)

    # --- Crossref API ---

    def search_journal(self, journal_name: str, max_results: int = 0) -> List[dict]:
        """Fetch all articles from a journal via Crossref and filter to book reviews.

        Uses cursor-based pagination (no offset limit) and no text query so that
        reviews whose titles lack the words "book review" are not missed.

        Args:
            journal_name: Crossref container-title to filter on.
            max_results: Stop after this many *total* items fetched (0 = no limit).
        """
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

                if page % 50 == 0:
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

        # Store unfiltered items for post-processing (e.g. symposium detection)
        self._last_all_items = all_items

        # Filter to book reviews client-side
        if self.JOURNALS.get(journal_name, {}).get('all_reviews'):
            reviews = all_items
        else:
            detection_mode = self.JOURNALS.get(journal_name, {}).get('detection_mode', 'all')
            reviews = [item for item in all_items if is_book_review(item, detection_mode)]
        self.log(f"  {journal_name}: {len(all_items)} items, {len(reviews)} book reviews")
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
        # Decode HTML entities (Crossref returns "Mind &amp; Language", "Philosophy &amp; Social Criticism", etc.)
        import html as _html
        container = _html.unescape(container)

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
        # PDCNET (10.5840/*, e.g. Teaching Philosophy): the doi.org link
        # redirects to an "oom/service" page behind Cloudflare ("Just a
        # moment…"). The article-specific content URL is derivable from the
        # imuse_id in the Crossref resource URL.
        if doi.startswith('10.5840/'):
            res_url = (crossref_item.get('resource', {}) or {}).get('primary', {}).get('URL', '')
            m = re.search(r'imuse_id=([a-z0-9_]+)', res_url, re.IGNORECASE)
            if m:
                imuse = m.group(1)
                prefix = imuse.split('_')[0]
                review_link = f'https://www.pdcnet.org/{prefix}/content/{imuse}'

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
            'Book Author First Name': re.sub(r'^by\s+', '', _normalize(parsed['book_author_first']), flags=re.IGNORECASE),
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
        # this is likely a symposium piece or research article, not a review.
        # Exceptions: journals with allow_author_replies=True keep these as
        # symposium contributions (Analysis precis, Metascience author replies).
        allow_replies = (container == 'Analysis' or
                         self.JOURNALS.get(container, {}).get('allow_author_replies'))
        if (record.get('Book Author Last Name') and record.get('Reviewer Last Name')
                and record['Book Author Last Name'].lower() == record['Reviewer Last Name'].lower()
                and record['Book Author First Name'].lower() == record['Reviewer First Name'].lower()
                and not allow_replies):
            return None

        self.stats['parsed_from_crossref'] += 1
        return record

    # --- OpenAlex book author lookup (Category B enrichment) ---

    def _normalize_for_comparison(self, title: str, drop_subtitle: bool = True) -> str:
        """Normalize a title for fuzzy comparison."""
        t = re.sub(r'<[^>]+>', '', title)
        if drop_subtitle:
            t = t.split(':')[0]  # drop subtitle
        t = re.sub(r'[^a-z0-9 ]', '', t.lower())
        return t.strip()

    def _title_match_score(self, book_title: str, openalex_title: str) -> float:
        """Score how well two titles match. Returns 0.0 to 1.0."""
        if not book_title or not openalex_title:
            return 0.0

        # First compare FULL titles (with subtitles) for a strong match
        full_book = self._normalize_for_comparison(book_title, drop_subtitle=False)
        full_oa = self._normalize_for_comparison(openalex_title, drop_subtitle=False)
        if full_book and full_oa:
            if full_book == full_oa:
                return 1.0
            if full_book.startswith(full_oa) or full_oa.startswith(full_book):
                # Prefer longer overlap
                return 0.95 * min(len(full_book), len(full_oa)) / max(len(full_book), len(full_oa))
            full_book_words = set(full_book.split())
            full_oa_words = set(full_oa.split())
            if full_book_words and full_oa_words:
                full_overlap = len(full_book_words & full_oa_words) / max(len(full_book_words), len(full_oa_words))
                if full_overlap > 0.8:
                    return full_overlap * 0.95

        # Fall back to main-title-only comparison
        norm_book = self._normalize_for_comparison(book_title)
        norm_oa = self._normalize_for_comparison(openalex_title)
        if not norm_book or not norm_oa:
            return 0.0
        if norm_book == norm_oa:
            return 0.8  # Good but not as confident as full-title match
        if norm_book.startswith(norm_oa) or norm_oa.startswith(norm_book):
            return 0.7 * min(len(norm_book), len(norm_oa)) / max(len(norm_book), len(norm_oa))
        book_words = set(norm_book.split())
        oa_words = set(norm_oa.split())
        if not book_words:
            return 0.0
        overlap = len(book_words & oa_words) / max(len(book_words), len(oa_words))
        return overlap * 0.6

    def _titles_match(self, book_title: str, openalex_title: str) -> bool:
        """Check if two book titles are a reasonable match."""
        return self._title_match_score(book_title, openalex_title) >= 0.5

    def lookup_book_author(self, book_title: str, review_year: int = 0) -> Optional[Tuple[str, str]]:
        """
        Look up the author of a book via OpenAlex API.
        Returns (first_name, last_name) or None if not found.

        Args:
            book_title: The book title to search for.
            review_year: Year the review was published (used to prefer books
                         published shortly before the review when multiple
                         books share the same title).
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
                    'api_key': os.getenv('OPENALEX_API_KEY', ''),
                    'mailto': self.crossref_email,
                },
                timeout=15,
            )
            if resp.status_code != 200:
                if resp.status_code == 429:
                    self.log(f"  OpenAlex rate limited (429)", "WARNING")
                return None

            results = resp.json().get('results', [])
            # Score all results and pick the best match
            best_score = 0.0
            best_year_penalty = float('inf')
            best_author = None
            for result in results:
                oa_title = result.get('title', '')
                score = self._title_match_score(book_title, oa_title)
                if score < 0.5:
                    continue
                authorships = result.get('authorships', [])
                if not authorships:
                    continue
                author = authorships[0].get('author', {})
                display_name = author.get('display_name', '')
                if not display_name:
                    continue
                parts = display_name.split()
                if not parts:
                    continue

                # When multiple books have the same score, prefer the one
                # published closest to (but not after) the review year.
                # A book reviewed in 2023 most likely came out 2020-2023.
                oa_year = result.get('publication_year') or 0
                if review_year and oa_year:
                    year_diff = review_year - oa_year
                    # Penalize books published after the review (unlikely)
                    # and books published long before the review
                    year_penalty = abs(year_diff) if year_diff >= 0 else 100
                else:
                    year_penalty = 50  # Unknown year — neutral

                # Pick this result if it has a higher score, or same score
                # but better year proximity
                if (score > best_score
                        or (score == best_score and year_penalty < best_year_penalty)):
                    if len(parts) >= 2:
                        best_score = score
                        best_year_penalty = year_penalty
                        best_author = (' '.join(parts[:-1]), parts[-1])
                    elif len(parts) == 1:
                        best_score = score
                        best_year_penalty = year_penalty
                        best_author = ('', parts[0])
            return best_author
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
        consecutive_failures = 0
        max_consecutive_failures = 30
        for i, record in enumerate(needs_author):
            if i > 0 and i % 100 == 0:
                self.log(f"  OpenAlex progress: {i}/{len(needs_author)} ({found} found)")

            # Extract review year to help disambiguate same-titled books
            review_year = 0
            pub_date = record.get('Publication Date', '')
            if pub_date and len(pub_date) >= 4:
                try:
                    review_year = int(pub_date[:4])
                except ValueError:
                    pass
            author = self.lookup_book_author(record['Book Title'], review_year=review_year)
            if author:
                record['Book Author First Name'] = author[0]
                record['Book Author Last Name'] = author[1]
                found += 1
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    self.log(f"  OpenAlex: {max_consecutive_failures} consecutive misses — likely rate limited, aborting", "WARNING")
                    break

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
        consecutive_failures = 0
        max_consecutive_failures = 5

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
                if resp.status_code == 429:
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                        self.log(f"  S2: {max_consecutive_failures} consecutive rate limits — aborting", "WARNING")
                        break
                    wait = min(30, 5 * consecutive_failures)
                    self.log(f"  S2 rate limited, waiting {wait}s ({consecutive_failures}/{max_consecutive_failures})", "WARNING")
                    time.sleep(wait)
                    continue
                if resp.status_code != 200:
                    self.log(f"  S2 batch error: {resp.status_code}", "WARNING")
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                        self.log(f"  S2: {max_consecutive_failures} consecutive errors — aborting", "WARNING")
                        break
                    time.sleep(2.0)
                    continue

                consecutive_failures = 0
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
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    self.log(f"  S2: {max_consecutive_failures} consecutive errors — aborting", "WARNING")
                    break

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

    # --- Analysis symposium detection ---

    def _detect_analysis_symposia(self, raw_items: List[dict],
                                   all_records: List[Dict]) -> List[Dict]:
        """Detect book symposia in Analysis issues and create symposium entries.

        Analysis runs ~2 book symposia per issue. Format:
          1. Précis/Summary by book author (start)
          2. 2-4 response papers by other philosophers
          3. Reply/Defending by book author (end)

        Scans ALL raw Crossref items for anchor patterns, then takes all papers
        between the first and last anchor (by page number) in each issue.

        Args:
            raw_items: All Crossref items fetched for Analysis (unfiltered).
            all_records: Records already extracted by extract_review().

        Returns:
            List of new symposium records to append to all_records.
        """
        # Index raw items by (volume, issue)
        items_by_vol_iss = {}
        for item in raw_items:
            vol = item.get('volume', '')
            iss = item.get('issue', '')
            if vol and iss:
                items_by_vol_iss.setdefault((vol, iss), []).append(item)

        def _norm_title(title):
            """Collapse whitespace for pattern matching."""
            return re.sub(r'\s+', ' ', title).strip()

        def _family_surname(family_name):
            """Extract the actual surname from a Crossref family name.

            Crossref encodes Asian names inconsistently: the family field
            may be just "Nguyen" or "Thi Nguyen".  Use the last word.
            Also strip Jr/Sr suffixes.
            """
            name = re.sub(r',?\s*(Jr|Sr|III|IV|II)\.?\s*$', '', family_name,
                          flags=re.IGNORECASE).strip()
            parts = name.split()
            return parts[-1].lower() if parts else ''

        # Step 1: Find symposium start anchors
        start_anchors = []  # Précis / Summary entries

        for item in raw_items:
            title = (item.get('title', ['']) or [''])[0]
            norm = _norm_title(title)
            vol = item.get('volume', '')
            iss = item.get('issue', '')
            if not vol or not iss:
                continue

            clean = re.sub(r'<[^>]+>', '', norm).strip()

            # Start: standalone "Summary" / "Précis" / "Precis"
            if clean.lower() in ('précis', 'precis', 'summary'):
                start_anchors.append(item)
                continue

            # Start: "<i>BookTitle</i> , By AuthorName" where Crossref
            # author matches the "By" author (= book author writing precis)
            precis_m = re.search(
                r'<i>.+?</i>\s*,\s*[Bb]y\s+(.+?)$', norm
            )
            if precis_m:
                by_author = precis_m.group(1).strip()
                # Strip Jr/Sr before parsing
                by_author_clean = re.sub(
                    r',?\s*(Jr|Sr|III|IV|II)\.?\s*$', '', by_author,
                    flags=re.IGNORECASE
                ).strip()
                cr_authors = item.get('author', [])
                if cr_authors:
                    cr_surname = _family_surname(
                        cr_authors[0].get('family', '')
                    )
                    _, by_last, _ = _extract_first_author(by_author_clean)
                    if by_last and cr_surname and (
                        by_last.lower() == cr_surname
                    ):
                        start_anchors.append(item)
                        continue

            # Start: "<i>Title: Précis</i>" pattern
            if re.search(r'<i>[^<]*[Pp]r[eé]cis[^<]*</i>', norm):
                start_anchors.append(item)
                continue

        if not start_anchors:
            self.log("  Analysis: no symposium anchors found")
            return []

        # Step 1b: Find end anchors only in issues that have start anchors
        start_vol_iss_set = {
            (a.get('volume', ''), a.get('issue', '')) for a in start_anchors
        }
        end_anchors = []    # Defending / Reply entries

        for item in raw_items:
            vol = item.get('volume', '')
            iss = item.get('issue', '')
            if (vol, iss) not in start_vol_iss_set:
                continue

            title = (item.get('title', ['']) or [''])[0]
            norm = _norm_title(title)
            clean = re.sub(r'<[^>]+>', '', norm).strip()

            # End: "Defending <i>...</i>"
            if re.match(r'(?i)defending\b', norm) and '<i>' in norm.lower():
                end_anchors.append(item)
                continue

            # End: title contains "Replies to" or "Reply to"
            if re.search(r'\bRepl(?:y|ies)\s+to\b', clean):
                end_anchors.append(item)
                continue

        self.log(f"  Analysis: found {len(start_anchors)} start + "
                 f"{len(end_anchors)} end symposium anchor(s)")

        # Build DOI index from already-extracted records for dedup/upgrade
        existing_by_doi = {r['DOI']: r for r in all_records if r.get('DOI')}

        # Step 2: Group anchors by (vol, issue) and find clusters
        # Only process issues that have at least one start anchor
        start_vol_iss = {}
        for a in start_anchors:
            key = (a.get('volume', ''), a.get('issue', ''))
            start_vol_iss.setdefault(key, []).append(a)

        end_vol_iss = {}
        for a in end_anchors:
            key = (a.get('volume', ''), a.get('issue', ''))
            end_vol_iss.setdefault(key, []).append(a)

        def _page_start(item):
            """Extract numeric start page from Crossref page field."""
            page = item.get('page', '')
            if page:
                start = page.split('-')[0].strip()
                try:
                    return int(start)
                except ValueError:
                    pass
            return None

        def _page_end(item):
            """Extract numeric end page."""
            page = item.get('page', '')
            if page and '-' in page:
                end = page.split('-')[-1].strip()
                try:
                    return int(end)
                except ValueError:
                    pass
            return _page_start(item)

        new_records = []

        for group_key, starts in start_vol_iss.items():
            vol, iss = group_key
            ends = end_vol_iss.get(group_key, [])

            # Sort issue items by page number
            issue_items = items_by_vol_iss.get(group_key, [])
            paged = [(ps, item) for item in issue_items
                     if (ps := _page_start(item)) is not None]
            paged.sort(key=lambda x: x[0])
            if not paged:
                continue

            # Find symposium boundaries: start page → end page
            # There may be multiple symposia per issue, so pair each start
            # with the closest matching end by the same author
            used_end_dois = set()
            symposia_ranges = []

            for s_anchor in starts:
                s_page = _page_start(s_anchor)
                if s_page is None:
                    continue

                # Find the matching end anchor (same surname, later pages)
                s_authors = s_anchor.get('author', [])
                s_surname = _family_surname(
                    s_authors[0].get('family', '') if s_authors else ''
                )

                best_end = None
                best_end_page = None
                for e_anchor in ends:
                    if e_anchor.get('DOI', '') in used_end_dois:
                        continue
                    e_page = _page_end(e_anchor)
                    if e_page is None or e_page <= s_page:
                        continue
                    e_authors = e_anchor.get('author', [])
                    e_surname = _family_surname(
                        e_authors[0].get('family', '') if e_authors else ''
                    )
                    if s_surname and e_surname and s_surname == e_surname:
                        if best_end_page is None or e_page < best_end_page:
                            best_end = e_anchor
                            best_end_page = e_page

                if best_end is not None:
                    used_end_dois.add(best_end.get('DOI', ''))
                    end_page = _page_end(best_end)
                    symposia_ranges.append((s_page, end_page))
                else:
                    # No matching end anchor — look for same author later
                    fallback_end = None
                    for _, item in paged:
                        item_start = _page_start(item)
                        if item_start is None or item_start <= s_page:
                            continue
                        i_authors = item.get('author', [])
                        i_surname = _family_surname(
                            i_authors[0].get('family', '') if i_authors else ''
                        )
                        if s_surname and i_surname and s_surname == i_surname:
                            fallback_end = item
                    if fallback_end:
                        symposia_ranges.append(
                            (s_page, _page_end(fallback_end))
                        )

            # Collect cluster items for each symposium range
            for range_start, range_end in symposia_ranges:
                cluster = [
                    item for p, item in paged
                    if p >= range_start and p <= range_end
                ]
                if len(cluster) < 2:
                    continue

                symposium_group = f"Analysis|{vol}|{iss}"
                cluster_titles = []

                for item in cluster:
                    doi = item.get('DOI', '')

                    # Skip if already in DB
                    if doi and db.doi_exists(doi):
                        continue

                    title = (item.get('title', ['']) or [''])[0]
                    clean_title = re.sub(r'<[^>]+>', '', title).strip()
                    clean_title = re.sub(r'\s+', ' ', clean_title)

                    # Get Crossref author
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
                    if review_link and not review_link.startswith('http'):
                        review_link = 'https://' + review_link

                    access_type = 'Open' if item.get('license') else 'Restricted'

                    # For precis entries, extract book author from title
                    book_author_first = ''
                    book_author_last = ''
                    norm = re.sub(r'\s+', ' ', title)
                    precis_match = re.search(
                        r'<i>.+?</i>\s*,\s*[Bb]y\s+(.+?)$', norm
                    )
                    if precis_match:
                        author_str = precis_match.group(1).strip()
                        first, last, _ = _extract_first_author(author_str)
                        book_author_first = first
                        book_author_last = last

                    # If DOI was already extracted as a regular review,
                    # upgrade it to symposium type
                    if doi in existing_by_doi:
                        existing_by_doi[doi]['entry_type'] = 'symposium'
                        existing_by_doi[doi]['symposium_group'] = symposium_group
                        cluster_titles.append(clean_title)
                        continue

                    record = {
                        'Book Title': _normalize(clean_title),
                        'Book Author First Name': _normalize(book_author_first),
                        'Book Author Last Name': _normalize(book_author_last),
                        'Reviewer First Name': _normalize(reviewer_first),
                        'Reviewer Last Name': _normalize(reviewer_last),
                        'Publication Source': 'Analysis',
                        'Publication Date': pub_date,
                        'Review Link': review_link,
                        'Review Summary': '',
                        'Access Type': access_type,
                        'DOI': doi,
                        'entry_type': 'symposium',
                        'symposium_group': symposium_group,
                    }
                    new_records.append(record)
                    cluster_titles.append(clean_title)

                if cluster_titles:
                    self.log(f"  Analysis symposium v{vol}i{iss}: "
                             f"{len(cluster_titles)} papers")
                    for t in cluster_titles:
                        self.log(f"    - {t[:80]}")

        if new_records:
            self.log(f"  Analysis symposia: {len(new_records)} new records")
        return new_records

    # --- Cluster-based symposium detection (Metascience-style) ---

    def _detect_cluster_symposia(self, journal_name: str,
                                  journal_records: List[Dict]) -> None:
        """Detect book symposia by clustering: 3+ reviews of the same book
        by 2+ distinct reviewers within 90 days.

        Modifies records in place by setting entry_type='symposium' and
        symposium_group. Adds [Author's Reply] or [Précis] suffix to
        book_title for entries where reviewer == book author.

        Args:
            journal_name: Journal being processed.
            journal_records: Records for this journal (modified in place).
        """
        def _norm_key(title: str) -> str:
            if not title:
                return ''
            # Strip author-tag suffixes
            t = re.sub(r"\s*\[.*?\]\s*", '', title)
            # Drop subtitle
            t = t.split(':')[0]
            # Lowercase alphanumeric
            t = re.sub(r'[^a-z0-9 ]', '', t.lower()).strip()
            return t

        # Group records by normalized book title
        by_book = {}
        for rec in journal_records:
            key = _norm_key(rec.get('Book Title', ''))
            if len(key) < 5:
                continue
            by_book.setdefault(key, []).append(rec)

        symposia_found = 0
        entries_tagged = 0

        for key, records in by_book.items():
            if len(records) < 3:
                continue

            # Parse dates and filter out records without dates
            dated = []
            for rec in records:
                pd = rec.get('Publication Date', '')
                try:
                    d = datetime.strptime(pd, '%Y-%m-%d')
                    dated.append((rec, d))
                except (ValueError, TypeError):
                    continue
            if len(dated) < 3:
                continue

            dated.sort(key=lambda x: x[1])
            span = (dated[-1][1] - dated[0][1]).days
            if span > 90:
                continue

            # Require 2+ distinct reviewers
            reviewers = set()
            for rec, _ in dated:
                key_r = ((rec.get('Reviewer First Name') or '').lower().strip(),
                         (rec.get('Reviewer Last Name') or '').lower().strip())
                reviewers.add(key_r)
            if len(reviewers) < 2:
                continue

            # Build symposium group ID
            year = dated[0][1].year
            slug_words = key.split()[:3]
            slug = '-'.join(slug_words)
            group = f"{journal_name}|{year}|{slug}"

            for rec, _ in dated:
                rec['entry_type'] = 'symposium'
                rec['symposium_group'] = group

                # Check if reviewer == book author (author contribution)
                baf = (rec.get('Book Author First Name') or '').lower().strip()
                bal = (rec.get('Book Author Last Name') or '').lower().strip()
                rf = (rec.get('Reviewer First Name') or '').lower().strip()
                rl = (rec.get('Reviewer Last Name') or '').lower().strip()
                if baf and bal and baf == rf and bal == rl:
                    # Determine label from the raw item title if available
                    # Default to [Précis] for opening essays
                    bt = rec.get('Book Title', '')
                    if '[' not in bt:
                        # Mark as précis by default; actual title might indicate reply
                        rec['Book Title'] = bt + " [Précis]"

                entries_tagged += 1
            symposia_found += 1

        if symposia_found:
            self.log(f"  {journal_name}: detected {symposia_found} symposia "
                     f"({entries_tagged} entries tagged)")

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
        analysis_raw_items = None

        for journal in journals:
            items = self.search_journal(journal, max_results=max_per_journal)

            # Save raw (unfiltered) items for Analysis symposium detection
            if journal == 'Analysis':
                analysis_raw_items = self._last_all_items

            journal_records = []
            for item in items:
                record = self.extract_review(item)
                if record:
                    journal_records.append(record)

            # Cluster-based symposium detection (per-journal opt-in)
            if self.JOURNALS.get(journal, {}).get('symposium_detection') == 'cluster':
                self._detect_cluster_symposia(journal, journal_records)

            all_records.extend(journal_records)

        # Detect Analysis book symposia from raw Crossref items
        if analysis_raw_items is not None:
            symposium_records = self._detect_analysis_symposia(
                analysis_raw_items, all_records
            )
            all_records.extend(symposium_records)

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

        duration = datetime.now() - start
        self.log(f"Completed in {str(duration).split('.')[0]}")

        self.results = all_records
        return all_records

    def _print_results(self, records: List[Dict]):
        """Print a compact summary of extracted records."""
        total = len(records)
        if total == 0:
            self.log("No records extracted")
            return
        with_title = sum(1 for r in records if r.get('Book Title'))
        with_author = sum(1 for r in records if r.get('Book Author Last Name'))
        journals = len({r.get('Publication Source', 'Unknown') for r in records})
        self.log(f"Extracted {total} reviews from {journals} journals — "
                 f"{with_title} with titles ({with_title*100//total}%), "
                 f"{with_author} with authors ({with_author*100//total}%)")


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
        'entry_type': record.get('entry_type', ''),
        'symposium_group': record.get('symposium_group', ''),
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
