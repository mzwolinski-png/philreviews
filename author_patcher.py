#!/usr/bin/env python3
"""
PhilReviews Author Patcher
Fixes missing author information in existing database records by re-scraping
the review pages with the improved CSS selector-based extraction.
"""

import requests
from bs4 import BeautifulSoup
import time
from ndpr_extraction import extract_review_data, is_review_page

import db


class AuthorPatcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PhilReviews/2.0 (academic research aggregator)'
        })

    def get_records_missing_authors(self):
        """Get all database records that are missing author information."""
        print("Fetching records from database...")
        missing = db.get_reviews_missing_authors()
        print(f"Found {len(missing)} records missing author information")
        return missing

    def extract_author_from_review(self, review_url):
        """Extract author information by re-scraping the review page."""
        try:
            print(f"  Fetching: {review_url}")
            response = self.session.get(review_url, timeout=15)

            if response.status_code == 404:
                print(f"    Page not found (404)")
                return None

            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            if not is_review_page(soup):
                print(f"    Not a review page")
                return None

            review_data = extract_review_data(soup, review_url)

            if review_data and (review_data.get('book_author_first') or review_data.get('book_author_last')):
                print(f"    Found author: {review_data.get('book_author_first', '')} {review_data.get('book_author_last', '')}")
                return {
                    'book_author_first': review_data.get('book_author_first', ''),
                    'book_author_last': review_data.get('book_author_last', ''),
                }
            else:
                print(f"    No author found")
                return None

        except Exception as e:
            print(f"    Error: {e}")
            return None

    def update_record_with_author(self, review_link, author_data):
        """Update a database record with author information."""
        first = author_data.get('book_author_first', '')
        last = author_data.get('book_author_last', '')
        db.update_author(review_link, first, last)
        print(f"    Updated record")
        return True

    def patch_missing_authors(self, limit=None):
        """Main function to patch missing author information."""
        print("Starting author patching...")

        records = self.get_records_missing_authors()

        if not records:
            print("No records missing author information!")
            return

        if limit:
            records = records[:limit]
            print(f"Processing first {limit} records...")

        successful = 0
        failed = 0

        for i, record in enumerate(records, 1):
            book_title = record.get('book_title', 'Unknown')
            review_url = record.get('review_link', '')

            print(f"\n{i}/{len(records)}: {book_title}")

            if not review_url:
                print(f"    No review URL")
                failed += 1
                continue

            author_data = self.extract_author_from_review(review_url)

            if author_data:
                self.update_record_with_author(review_url, author_data)
                successful += 1
            else:
                failed += 1

            time.sleep(1)

            if i % 25 == 0:
                total = successful + failed
                rate = successful / total * 100 if total > 0 else 0
                print(f"\nProgress: {i}/{len(records)} | {successful} updated, {failed} failed ({rate:.0f}%)")

        total = successful + failed
        rate = successful / total * 100 if total > 0 else 0
        print(f"\nPatching complete!")
        print(f"  Processed: {len(records)}")
        print(f"  Updated: {successful}")
        print(f"  Failed: {failed}")
        print(f"  Success rate: {rate:.0f}%")


def main():
    print("PhilReviews Author Patcher")
    print("=" * 50)

    patcher = AuthorPatcher()

    response = input("How many records to patch? (number or 'all'): ")

    if response.lower() == 'all':
        limit = None
    else:
        try:
            limit = int(response)
        except ValueError:
            print("Invalid input. Processing first 50.")
            limit = 50

    patcher.patch_missing_authors(limit)


if __name__ == "__main__":
    main()
