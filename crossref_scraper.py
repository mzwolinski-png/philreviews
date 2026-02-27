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

    # --- Format R: "Book Review:Title. Author Name" (old Ethics format, pre-1940) ---
    # Also handles "Book Review: Title" (no author, e.g. QJAE) → title-only with enrichment
    # Must check BEFORE stripping prefix, since the "Book Review:" is the signal
    br_colon = re.match(r'^Book\s*Review\s*:\s*(.+)', title, re.IGNORECASE)
    if br_colon:
        remainder = br_colon.group(1).strip()
        # Split at the last ". AuthorName" — author is 1-5 capitalized words at end
        author_end = re.search(r'\.\s+([A-Z][a-zA-Z.\s-]+?)$', remainder)
        if author_end:
            author_str = author_end.group(1).strip()
            # Validate it looks like a name (not a title fragment)
            if _looks_like_author_name(author_str):
                book_title = remainder[:author_end.start()].strip().rstrip('.')
                if book_title and len(book_title) > 3:
                    first, last, has_multiple = _extract_first_author(author_str)
                    if last:
                        return {
                            'book_title': book_title,
                            'book_author_first': first,
                            'book_author_last': last,
                            'is_edited_volume': False,
                            'has_multiple_authors': has_multiple,
                            'needs_doi_scrape': False,
                            'format': 'book_review_colon',
                        }
        # No author found — treat remainder as title-only (QJAE "Book Review: Title" format)
        if remainder and len(remainder) > 3:
            # Strip trailing italic markup if present
            clean_remainder = re.sub(r'<[^>]+>', '', remainder).strip().rstrip('.')
            if clean_remainder:
                return {
                    'book_title': clean_remainder,
                    'book_author_first': '',
                    'book_author_last': '',
                    'is_edited_volume': False,
                    'has_multiple_authors': False,
                    'needs_doi_scrape': True,
                    'format': 'book_review_colon_title_only',
                }

    # --- Format S: "Review of Author, Title" or "Review of Title, by Author" ---
    review_of_match = re.match(r'^Review\s+(?:of|Essay:)\s+(.+)', title, re.IGNORECASE)
    if review_of_match:
        remainder = review_of_match.group(1).strip()
        # "Review of Title, by Author" pattern
        by_match = re.match(r'^(.+?),\s+by\s+(.+?)$', remainder, re.IGNORECASE)
        if by_match:
            book_title = by_match.group(1).strip().rstrip('.')
            author_str = by_match.group(2).strip().rstrip('.')
            first, last, has_multiple = _extract_first_author(author_str)
            if book_title and last:
                return {
                    'book_title': book_title,
                    'book_author_first': first,
                    'book_author_last': last,
                    'is_edited_volume': False,
                    'has_multiple_authors': has_multiple,
                    'needs_doi_scrape': False,
                    'format': 'review_of_by',
                }
        # Title-only: "Review of Title"
        clean = re.sub(r'<[^>]+>', '', remainder).strip().rstrip('.')
        if clean and len(clean) > 3:
            return {
                'book_title': clean,
                'book_author_first': '',
                'book_author_last': '',
                'is_edited_volume': False,
                'has_multiple_authors': False,
                'needs_doi_scrape': True,
                'format': 'review_of_title_only',
            }

    # Normalize bold tags to italic (some journals use <b> instead of <i> for book titles)
    title = re.sub(r'<b>(.*?)</b>', r'<i>\1</i>', title)
    title = re.sub(r'<strong>(.*?)</strong>', r'<i>\1</i>', title)

    # Strip "Book Reviews" / "Book Review" / "Book Review:" / "Review of" prefix
    stripped = re.sub(r'^Book\s*Reviews?\s*:?\s*', '', title, flags=re.IGNORECASE)
    stripped = re.sub(r'^Review\s+of\s+', '', stripped)

    # --- Format A/B: <i>/<em> tags present ---
    italic_match = re.search(r'<(?:i|em)>(.*?)</(?:i|em)>', stripped)
    if italic_match:
        book_title = re.sub(r'<[^>]+>', '', italic_match.group(1)).strip()
        pre_italic = stripped[:italic_match.start()]
        pre_italic = re.sub(r'<[^>]+>', '', pre_italic)
        # Strip bibliographic noise: [1984], (1969), (Ed.), dates, prices, page counts
        pre_italic = re.sub(r',?\s*[\[\(]?\d{4}[\]\)]?\s*', '', pre_italic)
        pre_italic = re.sub(r'\(\s*\)', '', pre_italic)  # empty parens left after year removal
        pre_italic = re.sub(r',?\s*\([Ee]ds?\.?\)', '', pre_italic)  # (Ed.) / (Eds.)
        pre_italic = re.sub(r'[,.\s:;]+$', '', pre_italic).strip()

        # Text AFTER the closing </i> tag — e.g. "<i>Title</i>. Author Name"
        post_italic = stripped[italic_match.end():]
        # Truncate at start of second <i> tag (multi-review entries, e.g. HOPE)
        second_italic = re.search(r'<(?:i|em)>', post_italic)
        if second_italic:
            post_italic = post_italic[:second_italic.start()]
        post_italic = re.sub(r'<[^>]+>', '', post_italic)  # strip stray HTML
        post_italic = post_italic.replace('&amp;', '&')  # decode HTML entities
        # Remove leading punctuation/whitespace: ". Author Name" → "Author Name"
        post_italic = re.sub(r'^[,.\s:;]+', '', post_italic).strip()
        # Remove "translated by..." / "trans." suffix
        post_italic = re.split(r',?\s+translated\s+by\b', post_italic, flags=re.IGNORECASE)[0].strip()
        # Remove publisher/city/year tail: "Author Name. New York: Publisher, 2005..."
        post_italic = re.split(r'\.\s+(?:[A-Z][a-z]+:|\d{4}|pp\.)', post_italic)[0].strip()
        # Split at comma followed by publisher-like or city-like text
        post_italic = re.split(r',\s+(?:(?:Lawrence|Macmillan|Routledge|Oxford|Cambridge|Princeton|Harvard|Yale|MIT|Springer|Blackwell|Wiley|Penguin|Clarendon|Duckworth|Methuen|Allen|Longman|Chapman|Academic|Humanities|Nijhoff|Reidel|Kluwer)\b|Ltd\.)', post_italic)[0].strip()
        post_italic = re.split(r',\s+(?:New York|London|Cambridge|Oxford|Princeton|Chicago|Boston|Berkeley|Dordrecht|Leiden|The Hague|Ithaca|Toronto|Paris|Amsterdam|Berlin|Florence|Bloomington|Indianapolis|Philadelphia|Pittsburgh|Notre Dame|Englewood)', post_italic)[0].strip()
        # Split at comma followed by year
        post_italic = re.split(r',\s+\d{4}\b', post_italic)[0].strip()
        post_italic = re.sub(r'[,.\s]+$', '', post_italic).strip()

        if not pre_italic and book_title:
            # Check if italic text is an author name with ": Title" after it
            # (Kant-Studien format: <b>Author</b>: Title → <i>Author</i>: Title)
            raw_post = stripped[italic_match.end():]
            if raw_post.lstrip().startswith(':') and _looks_like_author_name(book_title):
                actual_title = re.sub(r'^[,.\s:;]+', '', raw_post).strip()
                # Strip publisher/city/year/page info from end
                actual_title = re.split(r'\.\s+(?:(?:Cambridge|Oxford|Princeton|Harvard|Yale|MIT|Springer|Routledge|Blackwell|Wiley|Penguin|Clarendon|Palgrave)\b|[A-Z][a-z]+\s+University\s+Press)', actual_title)[0].strip()
                actual_title = re.split(r'\.\s+(?:(?:New|West|St\.|San)\s+)?(?:York|London|Cambridge|Oxford|Princeton|Chicago|Boston|Berkeley|Dordrecht|Leiden|Ithaca|Toronto|Paris|Amsterdam|Berlin|Bloomington|Indianapolis|Philadelphia|Pittsburgh)\b', actual_title)[0].strip()
                actual_title = re.split(r',\s+\d{4}\b', actual_title)[0].strip()
                actual_title = re.split(r'\.\s+(?:\d{4}|pp\.)', actual_title)[0].strip()
                actual_title = re.sub(r',?\s+\d+\s*pp\.?.*$', '', actual_title).strip()
                actual_title = re.sub(r'[,.\s]+$', '', actual_title).strip()
                if actual_title and len(actual_title) > 10:
                    first, last, has_multiple = _extract_first_author(book_title)
                    if last:
                        return {
                            'book_title': actual_title,
                            'book_author_first': first,
                            'book_author_last': last,
                            'is_edited_volume': False,
                            'has_multiple_authors': has_multiple,
                            'needs_doi_scrape': False,
                            'format': 'italic_author_colon_title',
                        }

            # No text before <i>, but check for author after </i>
            # Handle "Edited by Author" in post_italic (may have subtitle prefix)
            edited_by_post = re.search(r'[Ee]dited\s+by\s+(.+)', post_italic)
            if edited_by_post:
                author_part = edited_by_post.group(1).strip()
                author_part = re.split(r'\.\s+(?=[A-Z][a-z]{2,}(?:[\s:,]|$)|\d{4})', author_part)[0].strip()
                author_part = re.split(r',\s+\d{4}\b', author_part)[0].strip()
                author_part = re.sub(r'[,.\s]+$', '', author_part).strip()
                first, last, has_multiple = _extract_first_author(author_part)
                if last and _looks_like_author_name((first + ' ' + last).strip() if first else last):
                    return {
                        'book_title': book_title,
                        'book_author_first': first,
                        'book_author_last': last,
                        'is_edited_volume': True,
                        'has_multiple_authors': has_multiple,
                        'needs_doi_scrape': False,
                        'format': 'italic_then_author',
                    }

            # Handle ", by Author. Edited by Editor" pattern (Mind format)
            by_match = re.match(r'^by\s+(.+)', post_italic, re.IGNORECASE)
            if by_match:
                author_part = by_match.group(1).strip()
                # Remove "Edited by ..." suffix
                author_part = re.split(r'\.\s*Edited\s+by\b', author_part, flags=re.IGNORECASE)[0].strip()
                # Remove publisher/city/year tail: "Author. Publisher, City, Year..."
                # Split at first ". " followed by a word that doesn't look like a name initial
                # (i.e., not just a single letter followed by a period)
                author_part = re.split(r'\.\s+(?=[A-Z][a-z]{2,}[\s:,]|\d{4}|\(|[A-Z]\.\s*&)', author_part)[0].strip()
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

    # --- Format H: "Title, written by Author" (JMP format) ---
    written_by_match = re.match(r'^(.+?),\s+written\s+by\s+(.+?)$', stripped, re.IGNORECASE)
    if written_by_match:
        book_title = written_by_match.group(1).strip()
        author_str = written_by_match.group(2).strip()
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
                'format': 'title_written_by_author',
            }

    # --- Format I: "Title, edited by Author" (JMP, others) ---
    edited_by_match = re.match(r'^(.+?),\s+edited\s+by\s+(.+?)$', stripped, re.IGNORECASE)
    if edited_by_match:
        book_title = edited_by_match.group(1).strip()
        author_str = edited_by_match.group(2).strip()
        author_clean = re.sub(r'[,.\s]+$', '', author_str).strip()
        first, last, has_multiple = _extract_first_author(author_clean)
        if book_title and last:
            return {
                'book_title': book_title,
                'book_author_first': first,
                'book_author_last': last,
                'is_edited_volume': True,
                'has_multiple_authors': has_multiple,
                'needs_doi_scrape': False,
                'format': 'title_edited_by_author',
            }

    # --- Format I2: "Title Edited by Author Publisher, Year, Pages" (no comma before Edited) ---
    edited_mid = re.match(r'^(.+?)\s+[Ee]dited\s+by\s+(.+)', stripped)
    if edited_mid:
        book_title = edited_mid.group(1).strip().rstrip(',.')
        author_str = edited_mid.group(2).strip()
        # Strip publisher/price/year/page info from end of author string
        author_str = re.split(r'\s+(?=[A-Z][a-z]{3,}[,:]\s)', author_str)[0]  # City: or Publisher,
        author_str = re.sub(r'\s+\d{4}.*$', '', author_str)
        author_str = re.sub(r',\s*\d+\s*pp\.?.*$', '', author_str)
        author_str = re.sub(r'\s*[\$£][\d.]+.*$', '', author_str)
        author_str = re.sub(r'[,.\s]+$', '', author_str).strip()
        # "Edited by" lists are "First Last, First Last and First Last" format
        # Extract just the first editor
        has_multiple = ',' in author_str or ' and ' in author_str.lower()
        first_editor = re.split(r',\s+|\s+and\s+', author_str, maxsplit=1, flags=re.IGNORECASE)[0].strip()
        parts = first_editor.split()
        if len(parts) >= 2:
            first, last = ' '.join(parts[:-1]), parts[-1]
        elif len(parts) == 1:
            first, last = '', parts[0]
        else:
            first, last = '', ''
        if book_title and last and len(book_title) > 5 and _looks_like_author_name(first_editor):
            return {
                'book_title': book_title,
                'book_author_first': first,
                'book_author_last': last,
                'is_edited_volume': True,
                'has_multiple_authors': has_multiple,
                'needs_doi_scrape': False,
                'format': 'title_edited_by_mid',
            }

    # --- Format J: "Title. By Author. (Publisher...)" (Philosophy journal) ---
    # Matches patterns like:
    #   "Greek Skepticism. by Charlotte L. Stough. (Berkeley...)"
    #   "Space, Time and Stuff. By Frank Arntzenius. Oxford University Press, 2012..."
    #   "Title. By Robert R. Magliola, West Lafayette: Publisher. 1977. Pages."
    by_author_match = re.match(r'^(.+?)\.\s+[Bb]y\s+(.+)', stripped)
    if by_author_match:
        book_title = by_author_match.group(1).strip()
        author_str = by_author_match.group(2).strip()
        # Strip publisher/city/year/page info from author string
        # Split at ", City:" or ", City," or ". Publisher" or ". Year" or ". Pages"
        # Strip city/publisher after author: ", West Lafayette..." or ", Lawrence & Wishart..."
        # Split at ": City" or ": Publisher" (Theoria: "Author: Oxford University Press, Year.")
        author_str = re.split(r':\s+(?:(?:New|West|St\.|San|Los|La|Le|Fort|Ann|Baton|Notre)\s+)?(?:York|London|Cambridge|Oxford|Princeton|Chicago|Boston|Berkeley|Dordrecht|Leiden|Hague|Ithaca|Toronto|Paris|Amsterdam|Berlin|Florence|Bloomington|Indianapolis|Philadelphia|Pittsburgh|Dame|Lafayette|Haven|Bonaventure|Cliffs|Angeles|Francisco|Diego|Arbor|Rouge)\b', author_str)[0]
        author_str = re.split(r':\s+(?:Lawrence|Macmillan|Routledge|Oxford|Cambridge|Princeton|Harvard|Yale|MIT|Springer|Blackwell|Wiley|Penguin|Clarendon|Duckworth|Methuen|Allen|Longman|Chapman|Academic|Humanities|Nijhoff|Reidel|Kluwer|Ltd)\b', author_str)[0]
        author_str = re.split(r',\s+(?:(?:New|West|St\.|San|Los|La|Le|Fort|Ann|Baton|Notre)\s+)?(?:York|London|Cambridge|Oxford|Princeton|Chicago|Boston|Berkeley|Dordrecht|Leiden|Hague|Ithaca|Toronto|Paris|Amsterdam|Berlin|Florence|Bloomington|Indianapolis|Philadelphia|Pittsburgh|Dame|Lafayette|Haven|Bonaventure|Cliffs|Angeles|Francisco|Diego|Arbor|Rouge)\b', author_str)[0]
        author_str = re.split(r',\s+(?:Lawrence|Macmillan|Routledge|Oxford|Cambridge|Princeton|Harvard|Yale|MIT|Springer|Blackwell|Wiley|Penguin|Clarendon|Duckworth|Methuen|Allen|Longman|Chapman|Academic|Humanities|Nijhoff|Reidel|Kluwer|Ltd)\b', author_str)[0]
        # Split at ". Publisher/Year" but not after a single initial (e.g. "R. Magliola")
        pub_split = re.search(r'(?<![A-Z])\.\s+(?:\(|[A-Z][a-z]{3,}[\s:,]|\d{4}|[xivlc]+[,.]|\d+\s+p)', author_str)
        if pub_split:
            author_str = author_str[:pub_split.start()]
        # Split at ", year"
        author_str = re.split(r',\s+\d{4}\b', author_str)[0]
        # Clean trailing punctuation, honorifics etc.
        author_str = re.sub(r'[,.\s]+$', '', author_str).strip()
        # Remove parenthetical qualifications like "(ed.)" or degree abbreviations
        author_str = re.sub(r'\s*\([^)]*\)\s*', ' ', author_str).strip()
        is_edited = bool(re.search(r'\beds?\.?\b|\beditors?\b|\bEdited\b', author_str, re.IGNORECASE))
        author_clean = re.sub(r',?\s*\beds?\.?\s*$', '', author_str,
                              flags=re.IGNORECASE).strip().rstrip(',').strip()
        first, last, has_multiple = _extract_first_author(author_clean)
        if book_title and last and len(book_title) > 5 and _looks_like_author_name(author_clean):
            return {
                'book_title': book_title,
                'book_author_first': first,
                'book_author_last': last,
                'is_edited_volume': is_edited,
                'has_multiple_authors': has_multiple,
                'needs_doi_scrape': False,
                'format': 'title_by_author_parens',
            }

    # --- Format M: "Title. Par/By Author." (Dialogue French format) ---
    par_match = re.match(r'^(.+?)\.\s+[Pp]ar\s+(.+?)\.', stripped)
    if par_match:
        book_title = par_match.group(1).strip()
        author_str = par_match.group(2).strip()
        author_str = re.sub(r'[,.\s]+$', '', author_str).strip()
        is_edited = False
        first, last, has_multiple = _extract_first_author(author_str)
        if book_title and last and len(book_title) > 5:
            return {
                'book_title': book_title,
                'book_author_first': first,
                'book_author_last': last,
                'is_edited_volume': is_edited,
                'has_multiple_authors': has_multiple,
                'needs_doi_scrape': False,
                'format': 'title_par_author',
            }

    # --- Format L: "Title, Author Name. Publisher, Year, pages." (Econ & Phil) ---
    # E.g.: "Climate Matters: Ethics in a Warming World, John Broome. Norton, 2012, 224 pages."
    # Also: "Is Multiculturalism Bad for Women?. Susan Moller Okin. Princeton..."
    # The author name follows the title, separated by comma or period, then publisher follows.
    title_comma_author = re.match(
        r'^(.+?)[,.][ ]+([A-Z][a-zA-Z.\s-]{3,40}?)\.[ ]+(?:[A-Z][a-z]+[\s:,]|\()',
        stripped
    )
    if title_comma_author:
        book_title = title_comma_author.group(1).strip()
        # Remove trailing question marks from title that might have been split
        author_str = title_comma_author.group(2).strip()
        author_str = re.sub(r'[,.\s]+$', '', author_str).strip()
        is_edited = bool(re.search(r'\beds?\.?\b|\beditors?\b|\bEdited\b', author_str, re.IGNORECASE))
        author_clean = re.sub(r',?\s*\beds?\.?\s*$', '', author_str,
                              flags=re.IGNORECASE).strip().rstrip(',').strip()
        first, last, has_multiple = _extract_first_author(author_clean)
        if (book_title and last and len(book_title) > 5
                and _looks_like_author_name(author_clean)
                and len(author_clean.split()) <= 5
                and not _looks_like_author_name(book_title)):
            return {
                'book_title': book_title,
                'book_author_first': first,
                'book_author_last': last,
                'is_edited_volume': is_edited,
                'has_multiple_authors': has_multiple,
                'needs_doi_scrape': False,
                'format': 'title_comma_author',
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

    # --- Format C2: "Title (review)" with no author (Philosophy East and West) ---
    review_suffix = re.match(r'^(.+?)\s*\(review\)\s*$', stripped, re.IGNORECASE)
    if review_suffix and not re.search(r'\bby\b', stripped, re.IGNORECASE):
        book_title = review_suffix.group(1).strip()
        if book_title and len(book_title) > 3:
            return {
                'book_title': book_title,
                'book_author_first': '',
                'book_author_last': '',
                'is_edited_volume': False,
                'has_multiple_authors': False,
                'needs_doi_scrape': True,
                'format': 'title_review_suffix',
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
        # Strip publisher/city/year/price/page-count metadata from author string
        # Split at ". City:" or ". Publisher" or ". Year" patterns
        author_str = re.split(r'\.\s+(?:(?:New|West|St\.|San)\s+)?(?:York|London|Cambridge|Oxford|Princeton|Chicago|Boston|Berkeley|Dordrecht|Leiden|Ithaca|Toronto|Paris|Amsterdam|Berlin|Bloomington|Indianapolis|Philadelphia|Pittsburgh|Notre Dame|Basingstoke|Northampton|Malden|Lanham|Albany|Cham)(?:[\s:,/]|$)', author_str)[0]
        author_str = re.split(r'\.\s+(?:(?:Lawrence|Macmillan|Routledge|Blackwell|Springer|Penguin|Harvard|Yale|MIT|Clarendon|Wiley|Palgrave|Elgar|Rowman|Doubleday|Houghton|McGraw|Polity|Continuum|Broadview|Hackett|Sage|Brill|Ashgate|Verso|Beacon|Basic|Transaction|Liberty|Ludwig|Mises|Cato|Oxford|Cambridge|Princeton|Cornell|Columbia|Stanford|Chicago|Duke|Georgetown|University|Academic)\s)', author_str)[0]
        author_str = re.split(r'\.\s+\d{4}\b', author_str)[0]
        author_str = re.sub(r'\s*\d+\s*pp\.?.*$', '', author_str, flags=re.IGNORECASE)
        author_str = re.sub(r'\s*ISBN[:\s].*$', '', author_str, flags=re.IGNORECASE)
        author_str = re.sub(r'\s*[\$£]\d+.*$', '', author_str)
        author_str = author_str.strip().rstrip('.,;: ')
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

    # --- Format P: "Author's Title" or "Review of Author's Title" (EJPE style) ---
    # E.g. "Julian Reiss's Philosophy of economics: a contemporary introduction. Routledge, 2013"
    # E.g. "Review of Thomas Mulligan's Justice and the Meritocratic State. New York: Routledge, 2018"
    possessive = re.match(r"^(?:Review of )?(.+?)['\u2019]s\s+(.+)$", stripped)
    if possessive and not re.search(r'["\u0027\u201c\u2018]', possessive.group(1)):
        author_str = possessive.group(1).strip()
        book_title = possessive.group(2).strip()
        # Clean bibliographic metadata
        _cities_p = (r'New York|London|Oxford|Cambridge|Princeton|Lanham|Chicago|Ithaca'
                     r'|Philadelphia|Durham|Minneapolis|Abingdon|San Francisco|Berkeley'
                     r'|Stanford|New Haven|Cham')
        _pubs_p = (r'Oxford University Press|Cambridge University Press|Princeton University Press'
                   r'|Harvard University Press|Cornell University Press|Columbia University Press'
                   r'|University of Chicago Press|University of California Press|Stanford University Press'
                   r'|Yale University Press|MIT Press|Routledge|Bloomsbury|Random House'
                   r'|Palgrave Macmillan|Springer Nature|Springer|Odile Jacob|Allen Lane')
        book_title = re.sub(r'\s*\([^)]*(?:' + _pubs_p + r')[^)]*\)', '', book_title).strip()
        # Handle "Title. City (State): Publisher" or "Title. City: Publisher"
        book_title = re.split(r'\.\s+[A-Z][a-z]+(?:\s*\([^)]+\))?\s*[:,]\s', book_title)[0].strip()
        book_title = re.split(r'\.\s+(?:' + _pubs_p + r')', book_title)[0].strip()
        book_title = re.split(r',\s+(?:' + _pubs_p + r')', book_title)[0].strip()
        book_title = re.split(r',\s+[A-Z][a-z]+(?:\s*\([^)]+\))?\s*[:,]\s', book_title)[0].strip()
        book_title = re.split(r'\.\s+(?:ISBN|pp\b|\d+\s*pp|\d{4}\b)', book_title)[0].strip()
        book_title = re.split(r',\s+\d+\s*pp\b', book_title)[0].strip()
        book_title = re.sub(r'[.,]\s*$', '', book_title).strip()
        if _looks_like_author_name(author_str) and book_title and len(book_title) > 3:
            is_edited = bool(re.search(r'\beds?\.?\b|\beditors?\b', author_str, re.IGNORECASE))
            first, last, has_multiple = _extract_first_author(author_str)
            if last:
                return {
                    'book_title': book_title,
                    'book_author_first': first,
                    'book_author_last': last,
                    'is_edited_volume': is_edited,
                    'has_multiple_authors': has_multiple,
                    'needs_doi_scrape': False,
                    'format': 'possessive_author_title',
                }

    # --- Format Q: 'Author, "Title"' or "Author, 'Title'" (Philosophy in Review) ---
    # E.g. 'Thomas Kelly, "Bias: A Philosophical Study"'
    # E.g. "Michael Hviid Jacobsen, (ed.), \"Postmortal Society: Towards a Sociology of Immortality.\""
    quoted = re.match(r'^(.+?),?\s*(?:\([Ee]ds?\.?\)\s*\.?\s*,?\s*)?["\u0027\u201c\u2018](.{10,}?)["\u0027\u201d\u2019]\.?\s*$', stripped)
    if quoted:
        author_str = quoted.group(1).strip().rstrip(',').strip()
        # Remove editor markers from author string
        author_str = re.sub(r',?\s*\([Ee]ds?\.?\)\s*\.?', '', author_str).strip().rstrip(',').strip()
        # Remove "&amp;" artifacts
        author_str = author_str.replace('&amp;', '&')
        book_title = quoted.group(2).strip().rstrip('.')
        is_edited = bool(re.search(r'\([Ee]ds?\.?\)', stripped))
        if _looks_like_author_name(author_str) and book_title:
            first, last, has_multiple = _extract_first_author(author_str)
            if last:
                return {
                    'book_title': book_title,
                    'book_author_first': first,
                    'book_author_last': last,
                    'is_edited_volume': is_edited,
                    'has_multiple_authors': has_multiple,
                    'needs_doi_scrape': False,
                    'format': 'quoted_title',
                }

    # --- Format N: "Author, Title" (Journal of Value Inquiry style) ---
    # E.g. "Monica Mueller, Contrary to Thoughtlessness: Rethinking Practical Wisdom"
    # Author part: 1-4 words, looks like a name; Title part: at least 15 chars
    author_comma_title = re.match(r'^([A-Z][a-zA-Z.\s-]{2,40}?),\s+([A-Z].{14,})$', stripped)
    if author_comma_title:
        author_str = author_comma_title.group(1).strip()
        book_title = author_comma_title.group(2).strip()
        # Clean bibliographic metadata from title (publisher, city, page count, ISBN, price)
        _cities = r'New York|London|Oxford|Cambridge|Princeton|Lanham|Chicago|Ithaca|Philadelphia|Durham|Minneapolis'
        _pubs = (r'Oxford University Press|Cambridge University Press|Princeton University Press'
                 r'|Harvard University Press|Cornell University Press|Columbia University Press'
                 r'|Routledge|Bloomsbury|Lexington Books|MIT Press|Anthem Press')
        book_title = re.sub(r'\s*\([^)]*(?:' + _pubs + r')[^)]*\)(?:\s*,?\s*\d+\s*pages?\.?)?', '', book_title).strip()
        book_title = re.split(r'\.\s+(?:' + _cities + r')[,:]\s', book_title)[0].strip()
        book_title = re.split(r'\.\s+(?:' + _pubs + r')', book_title)[0].strip()
        book_title = re.split(r',\s+(?:' + _cities + r')[,:]\s', book_title)[0].strip()
        book_title = re.split(r'\.\s+(?:ISBN|pp\b|\d+\s*pp|\d{4}\b)', book_title)[0].strip()
        book_title = re.sub(r'[.,]\s*$', '', book_title).strip()
        if _looks_like_author_name(author_str):
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
                    'format': 'author_comma_title',
                }

    # --- Format O: "Author: Title" or "Author. Title" (Environmental Ethics format) ---
    # Also handles "Author, eds. Title" and "Author, ed.: Title"
    # Priority: ed(s). pattern first, then ". " split, then ": " split, then ", Title"

    # O-1: "Author, ed(s). Title" or "Author, ed(s).: Title"
    ee_eds_match = re.match(r'^(.+?),\s*eds?\.\s*:?\s*(.+)', stripped)
    if ee_eds_match:
        author_part = ee_eds_match.group(1).strip()
        title_part = ee_eds_match.group(2).strip()
        if _looks_like_author_name(author_part) and len(author_part.split()) <= 8 and len(title_part) > 5:
            first, last, has_multiple = _extract_first_author(author_part)
            if last:
                return {
                    'book_title': title_part,
                    'book_author_first': first,
                    'book_author_last': last,
                    'is_edited_volume': True,
                    'has_multiple_authors': has_multiple,
                    'needs_doi_scrape': False,
                    'format': 'ee_author_title',
                }

    # O-2: "Author. Title" (period separator — author ends with surname 3+ chars)
    dot_splits = [(m.start(), m.end()) for m in re.finditer(r'\.\s+', stripped)]
    for ds_start, ds_end in dot_splits:
        cand_author = stripped[:ds_start].strip()
        cand_title = stripped[ds_end:].strip()
        if not cand_title or not cand_title[0].isupper():
            continue
        last_word = cand_author.split()[-1] if cand_author.split() else ''
        if len(last_word) < 3:
            continue  # Likely an initial, not the split point
        if _looks_like_author_name(cand_author) and 2 <= len(cand_author.split()) <= 6 and len(cand_title) > 5:
            is_edited = bool(re.search(r'\beds?\.?\b|\beditors?\b', cand_author, re.IGNORECASE))
            first, last, has_multiple = _extract_first_author(cand_author)
            if last:
                return {
                    'book_title': cand_title,
                    'book_author_first': first,
                    'book_author_last': last,
                    'is_edited_volume': is_edited,
                    'has_multiple_authors': has_multiple,
                    'needs_doi_scrape': False,
                    'format': 'ee_author_title',
                }
        break  # Only try first valid split

    # O-3: "Author: Title" (colon separator — author is a name, not a book title)
    ee_colon_match = re.match(r'^(.+?):\s+(.+)', stripped)
    if ee_colon_match:
        cand_author = ee_colon_match.group(1).strip()
        cand_title = ee_colon_match.group(2).strip()
        # Only treat as author:title if the pre-colon part is a short name
        if (_looks_like_author_name(cand_author) and 2 <= len(cand_author.split()) <= 6
                and len(cand_title) > 5):
            is_edited = bool(re.search(r'\beds?\.?\b|\beditors?\b', cand_author, re.IGNORECASE))
            author_clean = re.sub(r',?\s*\beds?\.?\s*$', '', cand_author,
                                  flags=re.IGNORECASE).strip()
            first, last, has_multiple = _extract_first_author(author_clean)
            if last:
                return {
                    'book_title': cand_title,
                    'book_author_first': first,
                    'book_author_last': last,
                    'is_edited_volume': is_edited,
                    'has_multiple_authors': has_multiple,
                    'needs_doi_scrape': False,
                    'format': 'ee_author_title',
                }

    # O-4: "Title by Author" (without "(review)" suffix — EE uses this too)
    ee_by_match = re.match(r'^(.+?)\s+by\s+([A-Z].+?)(?:,\s*eds?\.)?$', stripped)
    if ee_by_match:
        cand_title = ee_by_match.group(1).strip()
        cand_author = ee_by_match.group(2).strip()
        if _looks_like_author_name(cand_author) and len(cand_title) > 5:
            is_edited = bool(re.search(r'\beds?\.?\b|\beditors?\b', cand_author, re.IGNORECASE))
            author_clean = re.sub(r',?\s*\beds?\.?\s*$', '', cand_author,
                                  flags=re.IGNORECASE).strip()
            first, last, has_multiple = _extract_first_author(author_clean)
            if last:
                return {
                    'book_title': cand_title,
                    'book_author_first': first,
                    'book_author_last': last,
                    'is_edited_volume': is_edited,
                    'has_multiple_authors': has_multiple,
                    'needs_doi_scrape': False,
                    'format': 'ee_author_title',
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
                                    'book received', 'book notes', 'book note',
                                    'book reviews:', 'reviews', 'review'):
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
    # Title fragments tend to have lowercase words or common nouns/adjectives
    non_name_words = {'the', 'a', 'an', 'of', 'on', 'in', 'for', 'and', 'to', 'from',
                      'review', 'book', 'symposium', 'critical', 'reflections',
                      'commentary', 'response', 'reply', 'essay', 'matter', 'body',
                      'special', 'is', 'what', 'how', 'why', 'case', 'against',
                      'beyond', 'toward', 'towards', 'between'}

    # Common nouns/adjectives that appear in titles but not as surnames
    title_words = {'nature', 'ethics', 'justice', 'ecology', 'environmental',
                   'religion', 'global', 'climate', 'autonomous', 'literature',
                   'engaging', 'doing', 'cheap', 'plant', 'animal', 'wild',
                   'poverty', 'growth', 'being', 'piano', 'extinction', 'new',
                   'connection', 'sustainability', 'change', 'world', 'earth',
                   'value', 'morality', 'resources', 'rights', 'land',
                   'marxism', 'stoic', 'african', 'desiring', 'inherent',
                   'intrinsic', 'social', 'disclosive', 'food'}

    # If most words are non-name words, this is probably a title fragment
    lower_words = [w.lower().rstrip('.,;:?!') for w in words]
    non_name_count = sum(1 for w in lower_words if w in non_name_words)
    if non_name_count >= len(words) / 2:
        return False

    # If any word is a common title word (and not a known surname), flag as suspicious
    # Allow it only if there are also clear name indicators (initials like "J." or "M.")
    title_word_count = sum(1 for w in lower_words if w in title_words)
    has_initial = any(re.match(r'^[A-Z]\.$', w) for w in words)
    if title_word_count > 0 and not has_initial:
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

def is_book_review(crossref_item: dict, detection_mode: str = 'all') -> bool:
    """Check if a Crossref work item is a book review.

    Args:
        crossref_item: Crossref API work item.
        detection_mode: Controls which heuristics are used.
            'all'        — use every pattern (default, good for EE/JVI-style journals)
            'italic_only' — only detect via italic tags or explicit "book review" text
                           (safe for journals whose article titles use colons/subtitles)
    """
    title = (crossref_item.get('title', ['']) or [''])[0].lower()

    # Exclude non-review items
    exclude = ['editorial:', 'announcing', 'comment on', 'response to', 'reply to',
               'correction', 'erratum', 'retraction', 'call for papers',
               'book notes', 'books received']
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
    # Bold tags: some journals use <b> instead of <i> for book titles (Kant-Studien, JBSP)
    bold_match = re.search(r'<(?:b|strong)>(.*?)</(?:b|strong)>', title)
    if bold_match:
        bold_text = re.sub(r'<[^>]+>', '', bold_match.group(1)).strip()
        if len(bold_text) >= 15:
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

    raw_title = (crossref_item.get('title', ['']) or [''])[0]

    # Pattern: 'Author, "Title"' or "Author, 'Title'" (Philosophy in Review)
    if re.match(r'''^[A-Z].+?,\s*(?:\(eds?\.?\)\s*,?\s*)?["'\u201c'].{10,}["'\u201d']''', raw_title):
        return True

    # Pattern: "<b>Author</b>: Title" (Kant-Studien review format)
    if re.match(r'^<b>[^<]{5,}</b>\s*:', raw_title):
        return True

    # Pattern: "Title. By AuthorName." (Heythrop Journal review format)
    if re.search(r'\.\s+By\s+[A-Z][a-z]', raw_title):
        return True

    # --- Name-based heuristics (skip for italic_only mode) ---
    if detection_mode == 'italic_only':
        return False

    # Pattern: "Author's Title..." (EJPE possessive format)
    if re.match(r"^(?:Review of )?[A-Z][a-z]+(?:\s[A-Z]\.?)* [A-Z][a-zA-Z-]+['\u2019]s\s", raw_title):
        return True

    # Pattern: "Title. By Author: Publisher, Year. Pages."
    if re.search(r'\d+\s*pp\b', raw_title, re.IGNORECASE):
        return True

    # Pattern: starts with "LastName, First. <i>Title</i>" (common Crossref book review format)
    author_comma_match = re.match(r'^([A-Z][a-zA-Z-]+),\s+([A-Z][a-z])', raw_title)
    if author_comma_match:
        surname = author_comma_match.group(1)
        if 2 <= len(surname) <= 20 and surname.lower() not in (
            'nature', 'ethics', 'justice', 'ecology', 'the', 'being', 'value',
            'animal', 'people', 'land', 'wild', 'extinction', 'poverty', 'growth'):
            return True

    # Pattern: "Title, by Author"
    if re.search(r',\s+by\s+[A-Z]', raw_title):
        return True

    # Pattern: "Author: Title" (Environmental Ethics format)
    colon_match = re.match(r'^([A-Z][a-zA-Z.\s,]+?):\s+([A-Z])', raw_title)
    if colon_match:
        name_part = colon_match.group(1).strip()
        words = name_part.split()
        if 2 <= len(words) <= 6 and _looks_like_author_name(name_part):
            return True

    # Pattern: "Author. Title" (Environmental Ethics format)
    dot_match = re.match(r'^([A-Z][a-zA-Z.\s,]+?)\.\s+([A-Z][a-z])', raw_title)
    if dot_match:
        name_part = dot_match.group(1).strip()
        words = name_part.split()
        last_word = words[-1] if words else ''
        if 2 <= len(words) <= 6 and len(last_word) >= 3 and _looks_like_author_name(name_part):
            return True

    # Pattern: "Author, eds. Title" (Environmental Ethics edited volume)
    if re.match(r'^[A-Z].+?,\s*eds?\.\s+[A-Z]', raw_title):
        return True

    # Pattern: "Title. City: Publisher, Year. Pages." (RAE/BEQ format)
    # Detect by presence of page count + publisher/city info
    if re.search(r'\d+\s*pp\b', raw_title, re.IGNORECASE):
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
        # --- Original journals ---
        # Category A: <i> tags with author before them
        'Ethics': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
        'Utilitas': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
        'Inquiry': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
        'Philosophy of Science': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
        'European Journal of Philosophy': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
        # Category C: "Title by Author (review)"
        'Journal of the History of Philosophy': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
        # Category E: "Title, by Author"
        'Australasian Journal of Philosophy': {'crossref_parseable': True, 'detection_mode': 'italic_only'},
        # Category B: <i>Title</i> only — book author from OpenAlex
        'The Philosophical Review': {'crossref_parseable': False, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
        # Category D: generic "Book Review" — enriched via Semantic Scholar
        'Mind': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},
        # Category F/D mix: older entries have "Title - Author", newer are generic
        'The Philosophical Quarterly': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},

        # --- New journals ---
        # "Title, Author. Publisher, Year, pages." or "<i>Title</i>, by Author"
        'Economics and Philosophy': {'crossref_parseable': True, 'openalex_enrichable': True},
        # "Book Review: Title, written/edited by Author" or "Author, Title (Publisher)"
        'Journal of Moral Philosophy': {'crossref_parseable': True, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
        # "Title. By Author. (Publisher, Year.)"
        'Philosophy': {'crossref_parseable': True, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
        # "Book Review: <i>Title</i>, by Author" or "Book Review: Title"
        'Political Theory': {'crossref_parseable': True, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
        # "Title, Author, Publisher" or "Title. Par Author." (French/English)
        'Dialogue': {'crossref_parseable': True, 'openalex_enrichable': True},
        # "Author <i>Title</i>. (Publisher, Year)" or "Author. Title. Pp."
        'Religious Studies': {'crossref_parseable': True},
        # "<i>Title</i>" or "Title, by Author"
        'Faith and Philosophy': {'crossref_parseable': True, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
        # "<i>Title</i>" embedded in text — often no author parseable
        'British Journal for the History of Philosophy': {'crossref_parseable': False, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
        # Mixed: "<i>Title</i>" — often no author parseable
        'The Journal of Aesthetics and Art Criticism': {'crossref_parseable': False, 'openalex_enrichable': True, 'detection_mode': 'italic_only'},
        # Generic "BOOK REVIEWS" — needs Semantic Scholar enrichment
        'The British Journal of Aesthetics': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},
        # Generic "Book reviews" or "Book Review" — needs enrichment
        'History and Philosophy of Logic': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},
        # Many generic "Book reviews" — needs enrichment
        'International Journal for Philosophy of Religion': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},
        # Mixed formats, many generic — needs enrichment
        'Journal of Applied Philosophy': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},
        # Mixed: some "Author: Title", many not parseable — needs enrichment
        'Continental Philosophy Review': {'crossref_parseable': False, 'semantic_scholar_enrichable': True},
        # Mixed: "Author Title. City, Publisher" — too inconsistent
        'Hypatia': {'crossref_parseable': False, 'semantic_scholar_enrichable': True, 'detection_mode': 'italic_only'},
        # "Author, Title" or generic "Book reviews" — needs enrichment for generic ones
        'The Journal of Value Inquiry': {'crossref_parseable': True, 'semantic_scholar_enrichable': True},
        # "Author: Title" or "Author. Title" or "Title by Author"
        'Environmental Ethics': {'crossref_parseable': True, 'openalex_enrichable': True},
        # "Review of Author's Title. Publisher..." or "Author's Title. Publisher..."
        'Erasmus Journal for Philosophy and Economics': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # "Review of Title, by Author" format — clean parsing
        'Ancient Philosophy': {
            'crossref_parseable': True,
            'detection_mode': 'italic_only',
        },
        # Dedicated review journal — Author, "Title" format; all entries are reviews
        'Philosophy in Review': {
            'crossref_parseable': True,
            'all_reviews': True,
        },

        # --- Journals added via italic_only detection ---
        # Article titles commonly use colons/subtitles, so name-based heuristics cause false positives.
        # "Author <i>Title</i>" or "<i>Title</i>. By Author" format
        'The British Journal for the Philosophy of Science': {
            'crossref_parseable': False, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Generic "Book Review"/"Review" titles — needs Semantic Scholar enrichment
        'Erkenntnis': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Dedicated book review journal — all entries are reviews
        # Uses italic tags + "- By Author" format; many plain title-only entries
        'Philosophical Books': {
            'crossref_parseable': False, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # "Title by Author (review)" or "Title (review)" format
        'Philosophy East and West': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # "Title by Author (review)" format — near-perfect parsing
        'The Review of Metaphysics': {
            'crossref_parseable': True,
            'detection_mode': 'italic_only',
        },
        # "Title (review)" format — title-only, authors via OpenAlex
        'Philosophy and Literature': {
            'crossref_parseable': False, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Philosophy journal with reviews — mainly italic + (review) format
        'Sophia': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # --- Economics / PPE journals ---
        # "Book Review: Title" format (newer), "Book reviews" generic (older)
        'Quarterly Journal of Austrian Economics': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },
        # "Book Review: Title" and italic tags — small Crossref footprint (43 DOIs)
        'Journal of Libertarian Studies': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # "<i>Title</i> by Author" format — standard italic detection
        'History of Political Economy': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # "Author, Title. City: Publisher, Year. Pages. Price" format
        'The Review of Austrian Economics': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },
        # "Title, by Author. Publisher, Year. Pages." format
        'Business Ethics Quarterly': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # "Book Review: Title" format — most need OpenAlex for book author
        'Political Theory': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },
        'Journal of Moral Philosophy': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },
        'Ethical Theory and Moral Practice': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # "BOOK REVIEW: Author. TITLE. Publisher, Year." format
        'Hypatia': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # "Author, Title. City: Publisher, Year, ISBN" format
        'Hypatia Reviews Online': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },

        # --- Tier 1: New journals (Feb 2026) ---
        # Generic "Book Review" titles — needs Semantic Scholar enrichment
        'Law and Philosophy': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # "Book review of Author's Title" — excellent descriptive titles
        'Phenomenology and the Cognitive Sciences': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # "Book Review: Title" format — parseable with Format R
        'Philosophy of the Social Sciences': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # "Book Review: Title" format
        'European Journal of Political Theory': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # --- Tier 2: New journals (Feb 2026) ---
        # Generic "Book Review" titles — needs Semantic Scholar enrichment
        'Bioethics': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # "Review Essay" and review format — use italic detection
        'The Review of Politics': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Generic "Book Review" titles — needs Semantic Scholar enrichment
        'Studies in History and Philosophy of Science': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Generic "Book Review" titles — needs Semantic Scholar enrichment
        'Philosophical Psychology': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Generic "Book Review" titles — needs Semantic Scholar enrichment
        'Public Choice': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # --- Non-Western philosophy ---
        # Generic "Book review" titles — needs Semantic Scholar enrichment
        'Journal of Indian Philosophy': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Generic "Book Reviews" / "Book review" titles
        'Asian Philosophy': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Generic "Book Reviews" titles
        'Dao': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # --- General philosophy (niche) ---
        # Generic "Book Review" titles
        'Metaphilosophy': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Generic "Book review" titles
        'Philosophia': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # --- Additional niche journals ---
        # Generic "Book Review" — large backlog from 1900s-1970s
        'The Monist': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Generic "Book Review Essay" / "Book Review" — education/ethics
        'Journal of Moral Education': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Mixed: "Title. By Author: Publisher, Year. pp." and "Book Received" — Scandinavian
        'Theoria': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },
        # Generic "Book Review" — AI/philosophy of mind
        'Minds and Machines': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Generic "Book Review"
        'Ratio': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # "Book Review: Title" or "Review of Title" — some parseable
        'Res Publica': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Generic "BOOK REVIEW" / "Book Review"
        'Philosophical Investigations': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Generic "BOOK REVIEWS"
        'Journal of Social Philosophy': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Generic "Book Review" — medieval philosophy
        'Vivarium': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Book reviews and critical notices — American pragmatism
        'Transactions of the Charles S. Peirce Society': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── History of Philosophy ─────────────────────────────────────
        # "Author. <i>Title</i>" format — history of philosophy (est. ~842 reviews)
        'Archiv für Geschichte der Philosophie': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Philosophy of Mathematics ─────────────────────────────────
        # "Author. <i>Title</i>" format — philosophy of math (est. ~568 reviews)
        'Philosophia Mathematica': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Logic / Epistemology ──────────────────────────────────────
        # "Author. <i>Title</i>" format — logic and epistemology (est. ~454 reviews)
        'Dialectica': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Continental / General Philosophy ──────────────────────────
        # "<i>Title</i>, by Author" format — continental/general (est. ~121 reviews)
        'International Journal of Philosophical Studies': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── European Philosophy ───────────────────────────────────────
        # "Author. <i>Title</i>" format — European analytic philosophy (est. ~40 reviews)
        'Grazer Philosophische Studien': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── 17th/18th Century Philosophy ──────────────────────────────
        # "<b>Author</b>: Title" format (bold normalized to italic) — Kant scholarship
        'Kant-Studien': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },
        # Leibniz Review — ISSN not indexed in Crossref, skipped

        # ── 19th Century / Continental Philosophy ─────────────────────
        # Nietzsche scholarship (est. ~68 reviews)
        'The Journal of Nietzsche Studies': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Hegel scholarship (est. ~45 reviews)
        'Hegel Bulletin': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Continental/American philosophy (est. ~46 reviews)
        'The Journal of Speculative Philosophy': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Philosophy of Language ────────────────────────────────────
        # Mind and language (est. ~85 reviews)
        'Mind &amp; Language': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Applied Ethics ────────────────────────────────────────────
        # Global bioethics (est. ~28 reviews)
        'Developing World Bioethics': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── African Philosophy ────────────────────────────────────────
        # Filosofia Theoretica — too few reviews in Crossref (~2), skipped
        # East African philosophy (est. ~10 reviews)
        'Thought and Practice': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── General Analytic Philosophy ───────────────────────────────
        # Analytic philosophy (est. ~19 reviews)
        'Acta Analytica': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── General / Broad Coverage ─────────────────────────────────
        # Southern Journal of Philosophy (est. ~181 reviews)
        'The Southern Journal of Philosophy': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Philosophical Forum (est. ~59 reviews)
        'The Philosophical Forum': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Philosophy of Education ──────────────────────────────────
        # Studies in Philosophy and Education (est. ~151 reviews)
        'Studies in Philosophy and Education': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Journal of Philosophy of Education (est. ~82 reviews)
        'Journal of Philosophy of Education': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Educational Philosophy and Theory (est. ~134 reviews)
        'Educational Philosophy and Theory': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Continental Philosophy (more) ─────────────────────────────
        # Journal of the British Society for Phenomenology (est. ~143 reviews)
        'Journal of the British Society for Phenomenology': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Ancient Philosophy ────────────────────────────────────────
        # Apeiron — ancient Greek philosophy (est. ~76 reviews)
        'Apeiron': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Political Philosophy ──────────────────────────────────────
        # Critical Review of Intl Social and Political Philosophy (est. ~58 reviews)
        'Critical Review of International Social and Political Philosophy': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Philosophy of Science ─────────────────────────────────────
        # Foundations of Science (est. ~35 reviews)
        'Foundations of Science': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Aesthetics ───────────────────────────────────────────────
        # British Journal of Aesthetics (est. ~192 reviews)
        'The British Journal of Aesthetics': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Social / Political Philosophy (more) ─────────────────────
        # Philosophy and Social Criticism (est. ~225 reviews)
        'Philosophy &amp; Social Criticism': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Constellations (est. ~34 reviews)
        'Constellations': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Philosophy of Religion (more) ─────────────────────────────
        # Neue Zeitschrift für Systematische Theologie (est. ~141 reviews)
        'Neue Zeitschrift für Systematische Theologie und Religionsphilosophie': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── History of Philosophy (more) ──────────────────────────────
        # Intellectual History Review (est. ~113 reviews)
        'Intellectual History Review': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Hume Studies (est. ~91 reviews)
        'Hume Studies': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Studia Leibnitiana (est. ~11 reviews)
        'Studia Leibnitiana': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Applied Ethics (more) ─────────────────────────────────────
        # Science and Engineering Ethics (est. ~68 reviews)
        'Science and Engineering Ethics': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── General Philosophy ────────────────────────────────────────
        # Synthese (est. ~104 reviews)
        'Synthese': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── History of Concepts ───────────────────────────────────────
        # Archiv für Begriffsgeschichte — ISSN maps to Philologus, skipped
        # History of Philosophy & Logical Analysis — ISSN maps to wrong journal, skipped

        # ── Analytic Philosophy ───────────────────────────────────────
        # Analysis (est. ~354 reviews)
        'Analysis': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Canadian Journal of Philosophy (est. ~126 reviews)
        'Canadian Journal of Philosophy': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Feminist Philosophy / Bioethics ───────────────────────────
        # International Journal of Feminist Approaches to Bioethics (est. ~122 reviews)
        'IJFAB: International Journal of Feminist Approaches to Bioethics': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Process Philosophy ────────────────────────────────────────
        # Process Studies (est. ~95 reviews)
        'Process Studies': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── Pragmatism ────────────────────────────────────────────────
        # European Journal of Pragmatism and American Philosophy (est. ~76 reviews)
        'European Journal of Pragmatism and American Philosophy': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── New journals (Feb 27 2026) ──────────────────────────────

        # Philosophy of Biology — NOT previously configured, ~64 reviews on Crossref
        'Biology and Philosophy': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },
        # Phenomenology — ~66 reviews, "Book review" prefix format
        'Husserl Studies': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },
        # Philosophy of Law — ~72 reviews, italic tag format
        'Oxford Journal of Legal Studies': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # General philosophy — ~38 reviews, "Book Notices" format
        'International Philosophical Quarterly': {
            'crossref_parseable': False, 'semantic_scholar_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Philosophy of Law — ~15 reviews
        'Ratio Juris': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },
        # General / interdisciplinary — ~8 reviews
        'Topoi': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },
        # General M&E — top journal, few reviews but important
        'Noûs': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Logic — few reviews but fills gap
        'Journal of Philosophical Logic': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },

        # ── New journals (Feb 27 2026, batch 2) ─────────────────────

        # Environmental philosophy — "Book Review: <i>Title</i>" format (~765 est.)
        'Environmental Values': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },
        # Theology/philosophy of religion — "Title. By Author. Publisher, Year" format
        # Use italic_only to block possessive/colon false positives; ". By " pattern still fires
        'The Heythrop Journal': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Leibniz scholarship — "Review of Author, Title. Publisher, Year" format (~143 est.)
        'The Leibniz Review': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },
        # Legal philosophy — "Author, <i>Title</i>" format (~50 est.)
        'Jurisprudence': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Ethics — "Book Reviews" section (~75 est.)
        'Ethical Perspectives': {
            'crossref_parseable': True, 'openalex_enrichable': True,
        },
        # Social epistemology — mixed formats (~437 est.)
        'Social Epistemology': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Logic — "Author, Title. Publisher, Year" format (~229 est. new beyond 29 in DB)
        'Studia Logica': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Neuroethics — mixed formats (~131 est. new beyond 9 in DB)
        'Neuroethics': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Ancient/classical philosophy — some reviews (~32 est. new beyond 140 in DB)
        'Phronesis': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Agricultural/environmental ethics — mixed formats (~280 est.)
        'Journal of Agricultural and Environmental Ethics': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Ethics — "Author, <i>Title</i>" format (~152 est.)
        'The Journal of Ethics': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Legal philosophy — mixed (~52 est.)
        'Legal Theory': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Consciousness studies — mixed (~84 est.)
        'Journal of Consciousness Studies': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
        # Political philosophy — mixed (~136 est.)
        'Social Philosophy and Policy': {
            'crossref_parseable': True, 'openalex_enrichable': True,
            'detection_mode': 'italic_only',
        },
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
        # this is likely a symposium piece or research article, not a review
        if (record.get('Book Author Last Name') and record.get('Reviewer Last Name')
                and record['Book Author Last Name'].lower() == record['Reviewer Last Name'].lower()
                and record['Book Author First Name'].lower() == record['Reviewer First Name'].lower()):
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
