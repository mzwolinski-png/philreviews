#!/usr/bin/env python3
"""
PhilReviews NDPR Scraper
Scrapes recent Notre Dame Philosophical Reviews and adds new reviews to the database.
"""

import requests
from bs4 import BeautifulSoup
import time
from ndpr_extraction import extract_review_data, is_review_page, is_valid_review_url

import db


class NDPRScraper:
    def __init__(self):
        self.base_url = "https://ndpr.nd.edu"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PhilReviews/2.0 (academic research aggregator)'
        })

    def get_recent_reviews(self, limit=10):
        """Get recent review URLs from NDPR and scrape each one."""
        reviews = []

        try:
            print("Getting recent review URLs from NDPR...")
            review_urls = []

            # Get links from the main page
            response = self.session.get(self.base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if href and '/reviews/' in href:
                    if not href.startswith('http'):
                        href = self.base_url + href
                    if is_valid_review_url(href):
                        review_urls.append(href)

            # Deduplicate and limit
            review_urls = list(dict.fromkeys(review_urls))[:limit]
            print(f"Found {len(review_urls)} review URLs to process")

            for i, url in enumerate(review_urls):
                print(f"Processing review {i+1}/{len(review_urls)}: {url}")
                try:
                    review_data = self._scrape_one(url)
                    if review_data:
                        reviews.append(review_data)
                        print(f"  OK: {review_data.get('book_title', 'Unknown')}")
                    else:
                        print(f"  SKIP: not a valid review page")
                    time.sleep(1)
                except Exception as e:
                    print(f"  ERROR: {e}")
                    continue

        except Exception as e:
            print(f"Error getting review URLs: {e}")

        return reviews

    def _scrape_one(self, url):
        """Scrape a single review page and return structured data."""
        response = self.session.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        return extract_review_data(soup, url)

    def add_to_db(self, reviews):
        """Add reviews to the SQLite database."""
        if not reviews:
            print("No reviews to add")
            return

        records = [_to_db_fields(r) for r in reviews]
        db.insert_reviews(records)
        print(f"Inserted {len(records)} reviews into database")

    def check_for_duplicates(self, new_reviews):
        """Filter out reviews that already exist in the database."""
        filtered = []
        for review in new_reviews:
            link = review.get('review_link', '').strip()
            if link and not db.review_link_exists(link):
                filtered.append(review)
            else:
                print(f"Skipping duplicate: {review.get('book_title', 'Unknown')}")
        return filtered


def _to_db_fields(review):
    """Convert internal review dict to snake_case DB column names."""
    fields = {
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
    return fields


def main():
    print("PhilReviews NDPR Scraper")
    print("=" * 50)

    scraper = NDPRScraper()

    print("Scraping Notre Dame Philosophical Reviews...")
    reviews = scraper.get_recent_reviews(limit=10)

    if reviews:
        print(f"\nFound {len(reviews)} valid reviews")
        print("-" * 30)

        for i, review in enumerate(reviews, 1):
            print(f"{i}. {review.get('book_title', 'Unknown')}")
            print(f"   Author: {review.get('book_author_first', '')} {review.get('book_author_last', '')}")
            print(f"   Reviewer: {review.get('reviewer_first', '')} {review.get('reviewer_last', '')}")
            print(f"   Date: {review.get('publication_date', 'Not found')}")
            print(f"   URL: {review.get('review_link', '')}")
            print()

        print("Checking for duplicates...")
        new_reviews = scraper.check_for_duplicates(reviews)

        if new_reviews:
            print(f"Adding {len(new_reviews)} new reviews to database...")
            scraper.add_to_db(new_reviews)
            print("Done!")
        else:
            print("No new reviews to add (all are duplicates)")
    else:
        print("No reviews found")

    print("=" * 50)
    print("Scraper finished!")


if __name__ == "__main__":
    main()
