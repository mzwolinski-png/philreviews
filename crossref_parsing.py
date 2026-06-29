"""Pure title/author parsing helpers for the Crossref scraper.

Extracted from crossref_scraper.py — these depend only on the stdlib
(re, urllib.parse.quote, typing), not on the scraper class or DB, so
they live here as standalone, unit-testable functions.
"""

import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote


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

    # --- Format SUB-A: Subtitle contains "Author: Title. City: Publisher, Year" (Metascience) ---
    if subtitle:
        sub_a = re.match(
            r'^(.+?):\s+(.+?)\.\s+(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?:\s+)?'
            r'(.+?),\s+(\d{4})',
            subtitle
        )
        if sub_a:
            author_str = sub_a.group(1).strip()
            book_title_str = sub_a.group(2).strip()
            # Remember if this is an edited volume BEFORE stripping annotations
            is_edited = bool(re.search(r'\(\s*[Ee]ds?\.?\s*\)|\b[Ee]ds?\b\.?', author_str))
            # Strip "(eds)", "(Eds.)", "(ed.)" annotations for name validation
            author_clean = re.sub(r'\s*\(\s*[Ee]ds?\.?\s*\)\s*', '', author_str).strip()
            # For multi-author lists (2+ authors separated by commas), extract just the first author
            # "A, B, C and D" → "A"; "A and B" → "A and B" (handled by _extract_first_author)
            first_author_str = author_clean
            has_multiple_commas = author_clean.count(',') >= 2 or (
                author_clean.count(',') >= 1 and ' and ' in author_clean.lower()
            )
            if has_multiple_commas:
                # Take first segment before any comma
                first_author_str = author_clean.split(',')[0].strip()
            if len(book_title_str) > 5 and _looks_like_author_name(first_author_str):
                first, last, has_multiple = _extract_first_author(first_author_str)
                # Track multiple authors at the original level
                if author_clean != first_author_str:
                    has_multiple = True
                if last:
                    return {
                        'book_title': book_title_str,
                        'book_author_first': first,
                        'book_author_last': last,
                        'is_edited_volume': is_edited,
                        'has_multiple_authors': has_multiple,
                        'needs_doi_scrape': False,
                        'format': 'SUB-A',
                    }

        # --- Format SUB-B: Subtitle contains "Author, Publisher, Year, pages, ISBN" (CPT) ---
        # Subtitle may start with book subtitle: "[Subtitle,] Author, Publisher, Year..."
        # Strategy: find publisher segment, take the segment immediately before it as author.
        _pub_kw = (r'Press|University|Books|Publishing|Verlag|Routledge|Springer|Polity|'
                   r'Palgrave|Bloomsbury|MIT|Verso|Penguin|Harper|Edinburgh')
        parts_sub = [p.strip() for p in subtitle.split(',')]
        pub_idx = None
        for i, part in enumerate(parts_sub):
            if re.search(_pub_kw, part, re.IGNORECASE):
                pub_idx = i
                break
        if pub_idx is not None:
            author_str = None
            is_edited = False
            if pub_idx >= 1:
                # Author is the segment immediately before the publisher
                candidate = parts_sub[pub_idx - 1].strip()
                edited_match = re.match(r'^[Ee]dited\s+by\s+(.+)', candidate)
                if edited_match:
                    candidate = edited_match.group(1).strip()
                    is_edited = True
                if _looks_like_author_name(candidate):
                    author_str = candidate
            elif pub_idx == 0:
                # "Author Publisher" without comma: extract name before publisher keyword
                name_match = re.match(r'^(.+?)\s+(?:' + _pub_kw + r')', parts_sub[0])
                if name_match and _looks_like_author_name(name_match.group(1).strip()):
                    author_str = name_match.group(1).strip()
            book_title_clean = re.sub(r'<[^>]+>', '', title).strip()
            # If title is generic ("Book Review" etc.), extract the book title
            # from the subtitle: it's the segment(s) between the author and publisher.
            generic_titles = {'book review', 'book reviews', 'review', 'reviews',
                              'book review.', 'books received', 'book notes'}
            if book_title_clean.lower().strip('.').strip() in generic_titles and author_str and pub_idx >= 1:
                # Take everything between author (pub_idx - 1) and publisher (pub_idx).
                # For Law & Philosophy format the title is entirely in parts_sub[pub_idx - 1]
                # is author — so title is in parts_sub between author and publisher? No:
                # subtitle format is "Author, Title possibly with commas (Publisher, Year)..."
                # The author is parts_sub[pub_idx - 1], so the title comes before that.
                # Actually for Law & Philosophy: "Author, Title (Publisher" — author=parts[0],
                # parts[1] = "Title (Publisher" so title is the part before "(" in parts[pub_idx].
                # Extract title from the publisher segment by splitting at '(' or first publisher word.
                pub_segment = parts_sub[pub_idx]
                # Split at "(" or " Publisher"
                title_match = re.match(r'^([^(]+?)\s*(?:\(|' + _pub_kw + r')', pub_segment)
                if title_match:
                    extracted = title_match.group(1).strip().rstrip(',').rstrip('.')
                    if len(extracted) > 5:
                        book_title_clean = extracted
            if author_str:
                first, last, has_multiple = _extract_first_author(author_str)
                if last:
                    return {
                        'book_title': book_title_clean,
                        'book_author_first': first,
                        'book_author_last': last,
                        'is_edited_volume': is_edited,
                        'has_multiple_authors': has_multiple,
                        'needs_doi_scrape': False,
                        'format': 'SUB-B',
                    }
            # Have a subtitle with publisher but couldn't parse author — use title as book title
            if book_title_clean and len(book_title_clean) > 3:
                return {
                    'book_title': book_title_clean,
                    'book_author_first': '',
                    'book_author_last': '',
                    'is_edited_volume': False,
                    'has_multiple_authors': False,
                    'needs_doi_scrape': True,
                    'format': 'SUB-B',
                }

    # --- Format R: "Book Review:Title. Author Name" (old Ethics format, pre-1940) ---
    # Also handles "Book Review: Title" (no author, e.g. QJAE) → title-only with enrichment
    # Must check BEFORE stripping prefix, since the "Book Review:" is the signal
    br_colon = re.match(r'^(?:Commissioned\s+)?Book\s*Review\s*:\s*(.+)', title, re.IGNORECASE)
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
        # No author found — if remainder has italic tags or "Author, Title" pattern,
        # let other formats (italic handler, Format N) try after prefix stripping
        if remainder and ('<i>' in remainder or '<em>' in remainder):
            pass  # Fall through to italic/other format handlers
        elif remainder and len(remainder) > 3:
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
    stripped = re.sub(r'^(?:Commissioned\s+)?Book\s*Reviews?\s*:?\s*', '', title, flags=re.IGNORECASE)
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
            # Kant-Studien et al.: "<b>Author:</b> Title. City: Publisher, Year.
            # Pages. ISBN" — the colon sits INSIDE the bold tag, so book_title
            # carries a trailing colon and the real title follows </i>.
            if book_title.rstrip().endswith(':'):
                author_str = book_title.rstrip().rstrip(':').strip()
                if _looks_like_author_name(author_str.replace(',', ' ')):
                    title_str = stripped[italic_match.end():]
                    title_str = re.sub(r'<[^>]+>', '', title_str)
                    title_str = re.sub(r'^[\s.:;,]+', '', title_str).strip()
                    # cut at "City: Publisher" / "City : Publisher" (allow space)
                    title_str = re.split(r'\.\s+[A-Z][a-zA-ZÀ-ſ]+\s*:\s', title_str)[0].strip()
                    title_str = re.split(r'\bISBN\b', title_str)[0].strip()
                    title_str = re.split(r',?\s+\d+\s*(?:pages|pp|Seiten)\b', title_str, flags=re.IGNORECASE)[0].strip()
                    title_str = re.split(r',\s+\d{4}\b', title_str)[0].strip()
                    title_str = re.sub(r'[,.\s]+$', '', title_str).strip()
                    if title_str and len(title_str) > 5:
                        first, last, has_multiple = _extract_first_author(author_str)
                        if last:
                            return {
                                'book_title': title_str,
                                'book_author_first': first,
                                'book_author_last': last,
                                'is_edited_volume': False,
                                'has_multiple_authors': has_multiple,
                                'needs_doi_scrape': False,
                                'format': 'italic_author_colon_inside',
                            }
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

        # Try to extract author from "Review of / symposium on / Thoughts on" patterns
        author_from_prefix = re.search(
            r'(?:review\s+of|symposium\s+on|book\s+symposium\s+on|'
            r'thoughts\s+on|commentary\s+on|comments\s+on|reflections\s+on|'
            r'notes\s+on|remarks\s+on|on)\s+'
            r'([A-Z][a-zA-Z.\s-]+?)(?:[\'\u2019]\s*s?\s*)?$',
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
    # E.g. "Thoughts on Joshua Ehrlich's The East India Company..."
    # E.g. "Knowledge and Power in the Age of Conciliation: On Joshua Ehrlich's ..."
    # Strip review-prefix phrases so the author name is extracted cleanly
    possessive_input = re.sub(
        r"^(?:Review of|Thoughts on|Commentary on|Comments on|Reflections on|"
        r"Notes on|Remarks on|Response to|Reply to|Rejoinder to|"
        r".+?:\s+On)\s+",
        '',
        stripped,
        flags=re.IGNORECASE,
    )
    possessive = re.match(r"^(.+?)['\u2019]s\s+(.+)$", possessive_input)
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
        # Only treat as author:title if the pre-colon part is a short name.
        # Extra guard: reject if the candidate author contains function words
        # (prepositions, articles, conjunctions) — real author names don't have
        # "to", "for", "the", etc. unless they're name particles like "de"/"von".
        _function_words = {'the', 'a', 'an', 'of', 'on', 'in', 'for', 'and', 'to',
                           'from', 'with', 'at', 'by', 'or', 'nor', 'but', 'is',
                           'are', 'was', 'not', 'no', 'its', 'their', 'our', 'all'}
        cand_lower = {w.lower() for w in cand_author.split()}
        has_function_word = bool(cand_lower & _function_words)
        if (_looks_like_author_name(cand_author) and 2 <= len(cand_author.split()) <= 6
                and len(cand_title) > 5 and not has_function_word):
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
    if len(words) < 2 or len(words) > 6:
        # Require at least 2 words (first + last name). Single-word "names"
        # like "Convention", "Pragmaticism", "Logicians" are almost always
        # title fragments, not author names.
        return False

    # Author names are short and mostly capitalized words
    # Title fragments tend to have lowercase words or common nouns/adjectives
    non_name_words = {'the', 'a', 'an', 'of', 'on', 'in', 'for', 'and', 'to', 'from',
                      'review', 'book', 'symposium', 'critical', 'reflections',
                      'commentary', 'response', 'reply', 'essay', 'matter', 'body',
                      'special', 'is', 'what', 'how', 'why', 'case', 'against',
                      'beyond', 'toward', 'towards', 'between', 'its', 'or',
                      'not', 'with', 'this', 'that', 'their', 'some', 'all'}

    # Common nouns/adjectives that appear in titles but not as surnames
    title_words = {'nature', 'ethics', 'justice', 'ecology', 'environmental',
                   'religion', 'global', 'climate', 'autonomous', 'literature',
                   'engaging', 'doing', 'cheap', 'plant', 'animal', 'wild',
                   'poverty', 'growth', 'being', 'piano', 'extinction', 'new',
                   'connection', 'sustainability', 'change', 'world', 'earth',
                   'value', 'morality', 'resources', 'rights', 'land',
                   'marxism', 'stoic', 'african', 'desiring', 'inherent',
                   'intrinsic', 'social', 'disclosive', 'food',
                   'convention', 'science', 'cognitive', 'moral', 'political',
                   'philosophical', 'nations', 'century', 'logicians', 'early',
                   'reasoners', 'modern', 'ancient', 'classical', 'theory',
                   'philosophy', 'metaphysics', 'epistemology', 'logic',
                   'consciousness', 'knowledge', 'language', 'pragmatism',
                   'canada', 'europe', 'america', 'asia', 'first', 'second'}

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
            'dialogue'   — like italic_only but also detects ALL-CAPS author names
                           in the title (Dialogue's distinctive review format)
            'bib_required' — only accept items with explicit bibliographic markers
                           (Pp., ISBN, "by Author Name", "(review)", explicit "book
                           review" / "review of"). Italic tags alone are NOT enough.
                           Use for journals like Hastings Center Report whose articles
                           frequently use italicized subtitles.
    """
    title = (crossref_item.get('title', ['']) or [''])[0].lower()
    subtitle = ((crossref_item.get('subtitle') or [''])[0] or '').lower()
    title_plus_sub = title + ' ' + subtitle

    # Exclude non-review items (check both title and subtitle)
    exclude = ['editorial:', 'announcing', 'comment on', 'response to', 'reply to',
               'correction', 'erratum', 'retraction', 'call for papers',
               'book notes', 'books received', 'brief notices', 'notes on our contributors',
               'general index', 'author responds']
    for pattern in exclude:
        if pattern in title_plus_sub:
            return False

    # Length heuristic: items > 15 pages are very rarely book reviews.
    # Skip the check when the title has explicit review framing (already
    # near-certain review) or when the page field is missing/non-numeric.
    page_field = crossref_item.get('page', '') or ''
    page_m = re.match(r'\s*(\d+)\s*[-–]\s*(\d+)', page_field)
    if page_m:
        first_pg = int(page_m.group(1))
        last_pg = int(page_m.group(2))
        page_count = last_pg - first_pg + 1
        # Strong-review signals that override the length check
        explicit = bool(
            'book review' in title or 'book reviews' in title or
            '(review)' in title or 'review of' in title or
            'critical notice' in title or 'reviewed work' in title or
            re.search(r'\bISBN\b|\b[Pp]p\.\s', (crossref_item.get('title', ['']) or [''])[0])
        )
        if page_count > 15 and not explicit:
            return False

    # bib_required mode: only accept items with EXPLICIT bibliographic markers.
    # Italic tags alone are NOT enough (Hastings/etc. use italic subtitles for
    # article-style content).
    raw_title_check = (crossref_item.get('title', ['']) or [''])[0]
    if detection_mode == 'bib_required':
        has_bib = (
            '(review)' in title or
            'book review' in title or 'book reviews' in title or
            'review of' in title or 'reviewed work' in title or
            'critical notice of' in title or
            bool(re.search(r'\bISBN\b|\b[Pp]p\.\s|\bpp\.\s\d', raw_title_check)) or
            # "Title by Author Name" with capitalized name after "by"
            bool(re.search(r'\bby\s+[A-Z][a-z]+\s+[A-Z][a-zA-Z\-]+', raw_title_check))
        )
        if not has_bib:
            return False

    # Positive indicators
    # Italic/bold tags suggest a book title, but only if they dominate the title
    # or appear in a review-like framing context. An italic phrase embedded in a
    # longer article title might just be a mention, not a review.
    raw_title_for_tags = (crossref_item.get('title', ['']) or [''])[0]
    plain_text = re.sub(r'<[^>]+>', '', raw_title_for_tags).strip()
    for tag_re in [r'<(?:i|em)>(.*?)</(?:i|em)>', r'<(?:b|strong)>(.*?)</(?:b|strong)>']:
        tag_match = re.search(tag_re, title)
        if tag_match:
            tag_text = re.sub(r'<[^>]+>', '', tag_match.group(1)).strip()
            if len(tag_text) >= 15:
                tag_ratio = len(tag_text) / max(len(plain_text), 1)
                starts_with_tag = raw_title_for_tags.lstrip().startswith(('<i>', '<em>', '<b>', '<strong>'))
                # Detect if tagged text is dominant or title starts with tag
                if tag_ratio > 0.5 or (starts_with_tag and tag_ratio > 0.3):
                    return True
                # Detect review-like framing: possessive before italic ("Author's <i>Title</i>")
                # or review verbs ("reading", "re-reading", "reviewing")
                # Only trigger if pre-tag text is short (< 60 chars of plain text),
                # to avoid matching articles that just mention "Author's <i>Book</i>" in passing
                pre_tag = raw_title_for_tags[:tag_match.start()].rstrip()
                pre_tag_plain = re.sub(r'<[^>]+>', '', pre_tag).strip()
                if re.search(r"['\u2019]s\s*$", pre_tag) and len(pre_tag_plain) < 60:
                    return True
                if re.search(r'\b(?:re-?reading|reviewing|review of)\s*$', pre_tag, re.IGNORECASE):
                    return True
                # Italic text at end of title (nothing substantial after closing tag)
                post_tag = raw_title_for_tags[tag_match.end():].strip()
                post_plain = re.sub(r'<[^>]+>', '', post_tag).strip()
                if len(post_plain) < 5 and tag_ratio > 0.25:
                    return True
    if '(review)' in title:
        return True

    indicators = ['book review', 'book reviews', 'review of', 'reviewed work',
                   'critical notice of']
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

    # Pattern: "Title. By/by/Par AuthorName." (Heythrop Journal / Thomist / Dialogue French format)
    if re.search(r'\.\s+(?:[Bb]y|[Pp]ar)\s+[A-Z][a-z]', raw_title):
        return True

    # Bibliographic markers: publisher names, ISBN, or page counts in the title
    # are strong signals of a book review citation — articles never contain these.
    # Safe even in italic_only mode.
    if re.search(r'\bISBN\b', raw_title):
        return True
    if re.search(r'\b[Pp]p\.\s', raw_title):
        return True
    publisher_re = r'(?:University Press|Oxford UP|Cambridge UP|Oxford University|Cambridge University|Princeton University|Harvard University|Yale University|Routledge|De Gruyter|Springer Verlag|Meiner|Clarendon Press|Bloomsbury|Palgrave|Johns Hopkins|MIT Press|Cornell University|Columbia University|Duke University|Edinburgh University|Stanford University|Blackwell|Brill|Nomos|Suhrkamp|Gallimard|Vrin|Alber|Felix Meiner|Klostermann|Duncker|Mohr Siebeck|Quodlibet|Olms)'
    if re.search(publisher_re, raw_title):
        return True

    # Dialogue-style detection: ALL-CAPS author names in the title
    # e.g. "Developing the Virtues JULIA ANNAS, DARCIA NARVAEZ ... Oxford: OUP, 2017"
    if detection_mode in ('all', 'dialogue'):
        if re.search(r'[A-Z]{3,}\s+[A-Z]{3,}', raw_title):
            return True

    # --- Name-based heuristics (skip for italic_only and dialogue modes) ---
    if detection_mode in ('italic_only', 'dialogue'):
        return False

    # Pattern: "Title by PersonName" at end (Thomist pre-2023 format)
    # Requires " by " followed by text that looks like a person's name, at end of title
    by_end = re.search(r'\s+by\s+(.+?)\s*$', raw_title)
    if by_end and by_end.start() > 10 and _looks_like_author_name(by_end.group(1).strip()):
        return True

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
            'animal', 'people', 'land', 'wild', 'extinction', 'poverty', 'growth',
            'criteria', 'analysis', 'freedom', 'democracy', 'knowledge',
            'autonomy', 'causation', 'identity', 'language', 'meaning',
            'reality', 'perception', 'inference', 'consciousness'):
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
