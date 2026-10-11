#!/usr/bin/env python3
"""
Scraper for the Marx & Philosophy Review of Books.

The journal deposits no DOIs, so Crossref never sees it. Its reviews archive
(https://marxandphilosophy.org.uk/reviews/, ten to a page, newest first) gives
everything we index on the listing itself:

    <article class="... type-review ...">
      <span class="authors"> <a rel="tag">Book Author</a> ... </span>
      <span class="title"> <em>Book Title</em> </span>
      Reviewed by <a href=".../reviewer/...">Reviewer</a>
      <span class="deemphasize small"> 8 October 2026 </span>
      <a class="link_all" href=".../reviews/22942_...-reviewed-by-.../">

so no review page needs fetching. The weekly run reads the first pages and
stops at the first page that holds nothing new; --backfill walks them all.
"""

import html as htmllib
import re
import sys
import time
from datetime import datetime

import requests

import db

JOURNAL_NAME = 'Marx and Philosophy Review of Books'
LISTING_URL = 'https://marxandphilosophy.org.uk/reviews/'

_ARTICLE = re.compile(r'<article\b[^>]*\btype-review\b.*?</article>', re.S)
_TAG = re.compile(r'<[^>]+>')
# A surname that keeps its particle: "Bruce Bueno de Mesquita", "Hent de Vries"
_PARTICLES = {'de', 'del', 'della', 'di', 'da', 'du', 'van', 'von', 'le', 'la'}
_SUFFIX = re.compile(r'^(.*?),?\s+(Jr\.?|Sr\.?|II|III|IV)$')


def _text(fragment: str) -> str:
    return re.sub(r'\s+', ' ', htmllib.unescape(_TAG.sub(' ', fragment))).strip()


def split_person(name: str) -> tuple:
    """'Kostas Axelos' -> ('Kostas', 'Axelos'); particles and Jr. stay with the surname."""
    # "K. Murali (Ajith)": the listing appends a pen name; index the name itself
    name = re.sub(r'\s*\([^)]*\)\s*$', '', name.strip())
    if not name:
        return '', ''
    suffix = ''
    m = _SUFFIX.match(name)
    if m:
        name, suffix = m.group(1), ' ' + m.group(2)
    parts = name.split()
    k = len(parts) - 1
    while k > 0 and parts[k - 1].lower() in _PARTICLES and parts[k - 1].islower():
        k -= 1
    return ' '.join(parts[:k]), ' '.join(parts[k:]) + suffix


def author_fields(names: list) -> tuple:
    """Project convention: first = everything but the final surname."""
    names = [n for n in names if n]
    if not names:
        return '', ''
    if len(names) == 1:
        return split_person(names[0])
    first, last = split_person(names[-1])
    return f"{', '.join(names[:-1])} and {first}".strip(), last


def parse_date(text: str) -> str:
    """'8 October 2026' -> '2026-10-08'; '' when the listing gives none."""
    try:
        return datetime.strptime(text.strip(), '%d %B %Y').strftime('%Y-%m-%d')
    except ValueError:
        return ''


def parse_listing(page_html: str) -> list:
    """All reviews on one listing page, as records ready for db.insert_reviews."""
    out = []
    for m in _ARTICLE.finditer(page_html):
        art = m.group(0)
        link = re.search(r'<a class="link_all" href="([^"]+)"', art)
        title = re.search(r'<span class="title">(.*?)</span>', art, re.S)
        if not link or not title or not _text(title.group(1)):
            continue
        authors_block = re.search(r'<span class="authors">(.*?)</span>', art, re.S)
        authors = [_text(a) for a in re.findall(r'<a [^>]*rel="tag"[^>]*>(.*?)</a>',
                                                authors_block.group(1) if authors_block else '', re.S)]
        reviewers = [_text(r) for r in re.findall(r'<a href="[^"]*/reviewer/[^"]*">(.*?)</a>', art, re.S)]
        # Older entries put a comments badge inside the same span as the date.
        date = re.search(r'<span class="deemphasize small">\s*([^<]+)', art)
        a_first, a_last = author_fields(authors)
        r_first, r_last = author_fields(reviewers)
        out.append({
            'book_title': _text(title.group(1)),
            'book_author_first_name': a_first,
            'book_author_last_name': a_last,
            'reviewer_first_name': r_first,
            'reviewer_last_name': r_last,
            'publication_source': JOURNAL_NAME,
            'publication_date': parse_date(_text(date.group(1))) if date else '',
            'review_link': link.group(1),
            'review_summary': '',
            'access_type': 'Open',
            'doi': None,
            'entry_type': 'review',
        })
    return out


class MarxPhilosophyScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'

    def fetch_page(self, n: int):
        """Listing page n, or None once the archive runs out (404)."""
        url = LISTING_URL if n == 1 else f'{LISTING_URL}page/{n}/'
        resp = self.session.get(url, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.text

    def run(self, dry_run=False, backfill=False, max_pages=400):
        found, new = 0, []
        for n in range(1, max_pages + 1):
            try:
                page = self.fetch_page(n)
            except Exception as e:
                print(f'  Marx & Philosophy: page {n} failed: {e}')
                break
            if page is None:
                break
            recs = parse_listing(page)
            if not recs:
                break
            found += len(recs)
            fresh = [r for r in recs if not db.review_link_exists(r['review_link'])]
            new.extend(fresh)
            # Newest first, so a page with nothing new means we have caught up.
            if not fresh and not backfill:
                break
            if backfill and n % 25 == 0:
                print(f'  Progress: {n} pages, {len(new)} new so far')
            time.sleep(1)
        if new and not dry_run:
            db.insert_reviews(new)
        print(f'Marx & Philosophy: {found} listed, {len(new)} new')
        return {'found': found, 'new': len(new), 'uploaded': 0 if dry_run else len(new)}


def main():
    scraper = MarxPhilosophyScraper()
    scraper.run(dry_run='--dry-run' in sys.argv, backfill='--backfill' in sys.argv)


if __name__ == '__main__':
    main()
