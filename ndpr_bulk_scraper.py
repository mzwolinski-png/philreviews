#!/usr/bin/env python3
"""
PhilReviews NDPR Bulk Historical Scraper
Discovers and scrapes all available NDPR reviews to build a comprehensive database.
"""

import requests
from bs4 import BeautifulSoup
import re
import sys
import time
import json
from urllib.parse import urljoin
from ndpr_extraction import extract_review_data, is_review_page, is_valid_review_url

import db


class NDPRBulkScraper:
    def __init__(self):
        self.base_url = "https://ndpr.nd.edu"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PhilReviews/2.0 (academic research aggregator)'
        })
        self.processed_urls = set()
        self.failed_urls = set()

    def discover_all_review_urls(self):
        """
        Discover all available review URLs from NDPR.
        Only collects actual <a href> links found on the site —
        never constructs URLs from title slugs.
        """
        print("Discovering all available review URLs...")

        all_urls = set()

        # Strategy 1: Crawl archive pages by year (with pagination)
        print("  Crawling archive pages...")
        for year in range(2002, 2027):
            year_url = f"{self.base_url}/reviews/archives/{year}/"
            try:
                year_urls = self._crawl_paginated(year_url, max_pages=30)
                if year_urls:
                    all_urls.update(year_urls)
                    print(f"    {year}: {len(year_urls)} reviews")
                else:
                    print(f"    {year}: no reviews found")
                time.sleep(0.3)
            except Exception as e:
                print(f"    {year}: error - {e}")
                continue

        # Strategy 2: Crawl the main reviews page
        print("  Crawling main reviews page...")
        main_urls = self._crawl_paginated(f"{self.base_url}/reviews/")
        all_urls.update(main_urls)
        print(f"    Found {len(main_urls)} from main page")

        # Strategy 3: Crawl the review archive index
        print("  Crawling review archive index...")
        archive_urls = self._crawl_paginated(f"{self.base_url}/review-archive/")
        all_urls.update(archive_urls)
        print(f"    Found {len(archive_urls)} from archive index")

        # Strategy 4: Homepage
        print("  Crawling homepage...")
        home_urls = self._extract_review_links_from_url(self.base_url)
        all_urls.update(home_urls)
        print(f"    Found {len(home_urls)} from homepage")

        # Validate all URLs
        valid_urls = {u for u in all_urls if is_valid_review_url(u)}

        print(f"Total unique review URLs discovered: {len(valid_urls)}")
        return sorted(valid_urls)

    def _extract_review_links(self, soup):
        """Extract review URLs from a parsed page. Only follows actual <a href> links."""
        urls = set()
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if not href:
                continue
            if not href.startswith('http'):
                href = urljoin(self.base_url, href)
            if is_valid_review_url(href):
                urls.add(href)
        return urls

    def _extract_review_links_from_url(self, url):
        """Fetch a URL and extract review links from it."""
        try:
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                return self._extract_review_links(soup)
        except Exception:
            pass
        return set()

    def _crawl_paginated(self, start_url, max_pages=20):
        """Crawl a page and follow pagination links by walking sequentially."""
        all_urls = set()

        current_url = start_url
        page_num = 0

        while page_num < max_pages:
            try:
                response = self.session.get(current_url, timeout=10)
                if response.status_code != 200:
                    break

                soup = BeautifulSoup(response.content, 'html.parser')
                new_urls = self._extract_review_links(soup)
                if not new_urls and page_num > 0:
                    break  # No more reviews on this page
                all_urls.update(new_urls)
                page_num += 1

                # Find the "next" page link
                next_url = None
                for link in soup.select('a[href*="page/"]'):
                    href = link.get('href', '')
                    full = urljoin(self.base_url, href)
                    match = re.search(r'page/(\d+)', full)
                    if match and int(match.group(1)) == page_num + 1:
                        next_url = full
                        break

                if not next_url:
                    break

                current_url = next_url
                time.sleep(0.3)

            except Exception as e:
                print(f"    Error crawling {current_url}: {e}")
                break

        return all_urls

    def scrape_review_batch(self, urls, batch_size=50):
        """Scrape a batch of reviews."""
        print(f"Processing batch of {len(urls)} reviews...")

        reviews = []
        failed = []

        for i, url in enumerate(urls):
            if url in self.processed_urls:
                continue

            try:
                print(f"  {i+1}/{len(urls)}: {url}")
                response = self.session.get(url, timeout=15)

                if response.status_code == 404:
                    print(f"    SKIP: 404 not found")
                    failed.append(url)
                    self.failed_urls.add(url)
                    continue

                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')

                if not is_review_page(soup):
                    print(f"    SKIP: not a review page")
                    failed.append(url)
                    self.failed_urls.add(url)
                    continue

                review_data = extract_review_data(soup, url)

                if review_data and review_data.get('book_title'):
                    reviews.append(review_data)
                    self.processed_urls.add(url)
                    print(f"    OK: {review_data.get('book_title', 'Unknown')}")
                else:
                    failed.append(url)
                    self.failed_urls.add(url)
                    print(f"    FAIL: could not extract data")

                time.sleep(0.2)

                # Save progress periodically
                if len(reviews) % 25 == 0 and reviews:
                    self.save_batch_to_db(reviews[-25:])

            except Exception as e:
                print(f"    ERROR: {e}")
                failed.append(url)
                self.failed_urls.add(url)
                continue

        # Save remaining reviews
        remaining = len(reviews) % 25
        if remaining > 0:
            self.save_batch_to_db(reviews[-remaining:])

        return reviews, failed

    def save_batch_to_db(self, reviews):
        """Save a batch of reviews to the SQLite database."""
        if not reviews:
            return

        print(f"Saving {len(reviews)} reviews to database...")

        records = []
        for review in reviews:
            review_url = review.get('review_link', '')
            if review_url and db.review_link_exists(review_url):
                continue

            records.append(_to_db_fields(review))

        if records:
            db.insert_reviews(records)
            print(f"  Saved {len(records)} reviews")


def _to_db_fields(review):
    """Convert internal review dict to snake_case DB column names."""
    return {
        'book_title': review.get('book_title', ''),
        'book_author_first_name': review.get('book_author_first', ''),
        'book_author_last_name': review.get('book_author_last', ''),
        'reviewer_first_name': review.get('reviewer_first', ''),
        'reviewer_last_name': review.get('reviewer_last', ''),
        'publication_source': review.get('publication_source', ''),
        'publication_date': review.get('publication_date', ''),
        'review_link': review.get('review_link', ''),
        'review_summary': review.get('review_summary', ''),
        'access_type': review.get('access_type', ''),
        'doi': review.get('doi', ''),
    }


def main():
    print("PhilReviews NDPR Bulk Historical Scraper")
    print("=" * 60)

    scraper = NDPRBulkScraper()

    # Step 1: Discover all review URLs
    all_urls = scraper.discover_all_review_urls()

    if not all_urls:
        print("No review URLs found!")
        return

    print(f"\nFound {len(all_urls)} total review URLs to process")

    response = input(f"Process all {len(all_urls)} reviews? (y/n): ") if sys.stdin.isatty() else 'y'
    if response.lower() != 'y':
        print("Cancelled.")
        return

    # Step 2: Process in batches
    batch_size = 100
    total_processed = 0
    total_successful = 0

    for i in range(0, len(all_urls), batch_size):
        batch_urls = all_urls[i:i+batch_size]
        print(f"\nBatch {i//batch_size + 1}/{(len(all_urls)-1)//batch_size + 1}")

        reviews, failed = scraper.scrape_review_batch(batch_urls, batch_size)

        total_processed += len(batch_urls)
        total_successful += len(reviews)

        print(f"  Batch: {len(reviews)} successful, {len(failed)} failed")
        if total_processed > 0:
            print(f"  Overall: {total_successful}/{total_processed} ({total_successful/total_processed*100:.1f}%)")

        # Save progress
        with open('scraper_progress.json', 'w') as f:
            json.dump({
                'total_urls': len(all_urls),
                'processed': total_processed,
                'successful': total_successful,
                'failed_urls': sorted(scraper.failed_urls)
            }, f, indent=2)

    print(f"\nBulk scraping completed!")
    print(f"  Total URLs: {len(all_urls)}")
    print(f"  Successful: {total_successful}")
    print(f"  Failed: {len(scraper.failed_urls)}")
    if all_urls:
        print(f"  Success rate: {total_successful/len(all_urls)*100:.1f}%")


if __name__ == "__main__":
    main()
