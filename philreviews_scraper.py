"""
PhilReviews - Enhanced Ethics Journal Scraper with Reporting and Pagination
"""

import requests
import re
import time
import json
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime

import db


class PhilReviewsScraper:
    def __init__(self):
        self.crossref_base_url = "https://api.crossref.org/works/"

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PhilReviews/1.0 (https://philreviews.org; mailto:your-email@domain.com)'
        })

        self.stats = {
            'dois_processed': 0,
            'successful_extractions': 0,
            'db_inserts': 0,
            'errors': 0,
            'edited_volumes_skipped': 0,
            'multiple_authors_found': 0,
            'flagged_entries': 0,
            'start_time': None
        }
        
        self.uploaded_entries = []
        self.flagged_entries = []
    
    def log(self, message: str, level: str = "INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {level}: {message}")
    
    def get_crossref_metadata(self, doi: str) -> Optional[Dict]:
        try:
            response = self.session.get(f"{self.crossref_base_url}{doi}")
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') == 'ok':
                return data.get('message')
            else:
                self.log(f"Crossref API returned non-OK status for {doi}", "WARNING")
                return None
                
        except Exception as e:
            self.log(f"Error fetching {doi}: {e}", "ERROR")
            self.stats['errors'] += 1
            return None
    
    def clean_author_string(self, author_str: str) -> str:
        author_str = author_str.replace(';', ',')
        author_str = re.sub(r',?\s*(eds?\.?|trans\.?|translator|editor)(\s|$)', '', author_str, flags=re.IGNORECASE)
        author_str = re.sub(r'[,.\s]+$', '', author_str)
        return author_str.strip()
    
    def extract_first_author(self, author_str: str) -> Tuple[str, str, bool]:
        author_str = self.clean_author_string(author_str)
        
        comma_count = author_str.count(',')
        has_jr_sr = bool(re.search(r',\s*(Jr|Sr)\.?', author_str, flags=re.IGNORECASE))
        effective_comma_count = comma_count - 1 if has_jr_sr else comma_count
        
        has_multiple = ' and ' in author_str.lower() or effective_comma_count > 1
        
        if has_multiple:
            if ',' in author_str:
                first_author = author_str.split(',')[0].strip()
                parts = first_author.split()
                if len(parts) >= 2:
                    first = ' '.join(parts[:-1])
                    last = parts[-1]
                    return first, last, has_multiple
                elif len(parts) == 1:
                    return '', parts[0], has_multiple
        
        author_str = self.clean_author_string(author_str)
        
        if ',' in author_str:
            parts = author_str.split(',')
            last = parts[0].strip()
            
            if len(parts) >= 2:
                first = parts[1].strip()
                if len(parts) >= 3:
                    jr_sr = parts[2].strip()
                    if re.match(r'(Jr|Sr)\.?$', jr_sr, flags=re.IGNORECASE):
                        last = f"{last}, {jr_sr}"
                return first, last, has_multiple
        
        parts = author_str.strip().split()
        if len(parts) >= 2:
            first = ' '.join(parts[:-1])
            last = parts[-1]
            return first, last, has_multiple
        elif len(parts) == 1:
            return '', parts[0], has_multiple
        
        return '', '', has_multiple
    
    def is_edited_volume(self, title: str) -> bool:
        if re.search(r',\s*eds?\.', title, flags=re.IGNORECASE):
            return True
        return False
    
    def validate_title(self, title: str) -> bool:
        if not title:
            return False
        clean_title = title.strip()
        if len(clean_title) < 3:
            return False
        garbage_patterns = [r'^eds?\.?$', r'^Jr\.?$', r'^Sr\.?$', r'^[A-Z]\.?$', r'^and\s+\w+$']
        for pattern in garbage_patterns:
            if re.match(pattern, clean_title, flags=re.IGNORECASE):
                return False
        return True
    
    def validate_name(self, name: str, min_length: int = 2) -> bool:
        if not name:
            return False
        clean_name = name.strip()
        if len(clean_name) < min_length:
            return False
        if re.match(r'^[^a-zA-Z]+$', clean_name):
            return False
        return True
    
    def flag_entry(self, entry: Dict, reason: str):
        entry['_flag_reason'] = reason
        self.flagged_entries.append(entry)
        self.stats['flagged_entries'] += 1
    
    def parse_book_info_from_title(self, title: str) -> Tuple[str, str, str, Dict]:
        """
        Parse book info from a Crossref title string.

        Crossref Ethics titles typically look like:
          Book ReviewsLastName, First. <i>Book Title</i>. Publisher, Year...
        The <i> tag reliably marks the book title.
        """
        book_title = ""
        book_author_first = ""
        book_author_last = ""
        metadata = {
            'is_edited_volume': False,
            'has_multiple_authors': False
        }

        # Normalize whitespace characters (nbsp, thin spaces, etc.)
        title = title.replace('\xa0', ' ').replace('\u2002', ' ')
        # Normalize Unicode dashes/hyphens
        title = title.replace('\u2010', '-').replace('\u2011', '-')
        title = title.replace('\u2013', '-').replace('\u2014', '-')

        # Strip "Book Reviews" / "Book Review" prefix
        title = re.sub(r'^Book\s*Reviews?\s*', '', title)

        # --- Primary strategy: use <i>/<em> tags as structural markers ---
        italic_match = re.search(r'<(?:i|em)>(.*?)</(?:i|em)>', title)
        if italic_match:
            book_title = italic_match.group(1).strip()
            # Clean any nested HTML from the book title
            book_title = re.sub(r'<[^>]+>', '', book_title).strip()
            # Normalize smart quotes
            book_title = book_title.replace('\u2018', "'").replace('\u2019', "'")
            book_title = book_title.replace('\u201c', '"').replace('\u201d', '"')

            # Author is everything before the <i> tag
            author_section = title[:italic_match.start()]
            # Clean up: remove trailing punctuation, commas, dots, spaces
            author_section = re.sub(r'[,.\s]+$', '', author_section).strip()
            # Remove stray HTML
            author_section = re.sub(r'<[^>]+>', '', author_section).strip()

            # Detect edited volumes
            metadata['is_edited_volume'] = bool(
                re.search(r'\beds?\.?\b|\beditors?\b', author_section, re.IGNORECASE)
            )
            # Remove editor markers for name parsing
            author_clean = re.sub(
                r',?\s*\beds?\.?\s*$|\beditors?\s*$', '', author_section, flags=re.IGNORECASE
            ).strip().rstrip(',').strip()

            if author_clean:
                first, last, has_multiple = self.extract_first_author(author_clean)
                book_author_first = first
                book_author_last = last
                metadata['has_multiple_authors'] = has_multiple

            if self.validate_title(book_title) and self.validate_name(book_author_last):
                return book_title, book_author_first, book_author_last, metadata

        # --- Fallback: no <i> tags, try regex on plain text ---
        plain = re.sub(r'<[^>]+>', '', title).strip()
        plain = re.sub(r',\s*,+', ',', plain)

        # Try "LastName, FirstName. Book Title. Publisher..."
        fallback = re.match(r'^([A-Z][^.]+?)\.\s+([^.]+?)\.', plain)
        if fallback:
            author_section = fallback.group(1).strip()
            book_title = fallback.group(2).strip()

            metadata['is_edited_volume'] = bool(
                re.search(r'\beds?\.?\b', author_section, re.IGNORECASE)
            )
            author_clean = re.sub(
                r',?\s*\beds?\.?\s*$', '', author_section, flags=re.IGNORECASE
            ).strip().rstrip(',').strip()

            if author_clean and self.validate_title(book_title):
                first, last, has_multiple = self.extract_first_author(author_clean)
                book_author_first = first
                book_author_last = last
                metadata['has_multiple_authors'] = has_multiple

                if self.validate_name(book_author_last):
                    return book_title, book_author_first, book_author_last, metadata

        return book_title, book_author_first, book_author_last, metadata
    
    def is_book_review(self, crossref_data: Dict) -> bool:
        if not crossref_data:
            return False
        
        title = crossref_data.get('title', [''])[0].lower()
        
        exclude_patterns = [
            'editorial:', 'announcing', 'comment on', 'response to', 'reply to',
            'correction', 'erratum', 'retraction', 'would adopting', 'call for papers'
        ]
        
        for pattern in exclude_patterns:
            if pattern in title:
                return False
        
        book_review_indicators = ['book review', 'book reviews', 'review of', 'reviewed work']
        
        for indicator in book_review_indicators:
            if indicator in title:
                return True
        
        if re.search(r'^[A-Z][^,]+,\s+[A-Z]', crossref_data.get('title', [''])[0]):
            return True
        
        return False
    
    def extract_review_data(self, crossref_data: Dict) -> Dict:
        if not crossref_data:
            return {}
        
        reviewer_first = ""
        reviewer_last = ""
        if crossref_data.get('author') and len(crossref_data['author']) > 0:
            author = crossref_data['author'][0]
            reviewer_first = author.get('given', '')
            reviewer_last = author.get('family', '')
        
        pub_date = ""
        if crossref_data.get('issued') and crossref_data['issued'].get('date-parts'):
            date_parts = crossref_data['issued']['date-parts'][0]
            if len(date_parts) >= 1:
                year = date_parts[0]
                month = date_parts[1] if len(date_parts) > 1 else 1
                day = date_parts[2] if len(date_parts) > 2 else 1
                pub_date = f"{year:04d}-{month:02d}-{day:02d}"
        
        original_title = ""
        if crossref_data.get('title') and len(crossref_data['title']) > 0:
            original_title = crossref_data['title'][0]
        
        book_title, book_author_first, book_author_last, metadata = self.parse_book_info_from_title(original_title)
        
        if metadata.get('is_edited_volume'):
            self.stats['edited_volumes_skipped'] += 1
        
        if metadata.get('has_multiple_authors'):
            self.stats['multiple_authors_found'] += 1
        
        access_type = "Restricted"
        if crossref_data.get('license'):
            access_type = "Open"
        
        review_summary = crossref_data.get('abstract', '')
        if review_summary:
            review_summary = re.sub(r'<[^>]+>', '', review_summary)
        
        review_link = crossref_data.get('URL', '')
        if review_link and not review_link.startswith('http'):
            review_link = 'https://' + review_link
        
        return {
            'Book Title': book_title.strip() if book_title else '',
            'Book Author First Name': book_author_first.strip() if book_author_first else '',
            'Book Author Last Name': book_author_last.strip() if book_author_last else '',
            'Reviewer First Name': reviewer_first.strip() if reviewer_first else '',
            'Reviewer Last Name': reviewer_last.strip() if reviewer_last else '',
            'Publication Source': crossref_data.get('container-title', [''])[0] if crossref_data.get('container-title') else '',
            'Publication Date': pub_date,
            'Review Link': review_link,
            'Review Summary': review_summary.strip() if review_summary else '',
            'Access Type': access_type.strip(),
            'DOI': crossref_data.get('DOI', '')
        }
    
    def search_ethics_dois(self, max_items: int = 0) -> List[str]:
        """Fetch all Ethics articles via Crossref cursor pagination and filter to book reviews.

        Args:
            max_items: Stop after fetching this many total items (0 = no limit).
        """
        search_url = "https://api.crossref.org/works"
        all_items = []
        cursor = '*'
        page = 0

        while True:
            try:
                params = {
                    'filter': 'container-title:Ethics',
                    'rows': 100,
                    'cursor': cursor,
                    'mailto': 'your-email@domain.com',
                }
                response = self.session.get(search_url, params=params)
                response.raise_for_status()
                data = response.json()

                items = data.get('message', {}).get('items', [])
                if not items:
                    break
                all_items.extend(items)
                page += 1

                if page % 10 == 0:
                    self.log(f"Fetched {len(all_items)} Ethics articles so far...")

                if max_items and len(all_items) >= max_items:
                    all_items = all_items[:max_items]
                    break

                cursor = data.get('message', {}).get('next-cursor', '')
                if not cursor:
                    break

                time.sleep(0.5)
            except Exception as e:
                self.log(f"Error searching for DOIs: {e}", "ERROR")
                break

        book_review_dois = []
        for item in all_items:
            doi = item.get('DOI')
            if doi and self.is_book_review(item):
                book_review_dois.append(doi)

        self.log(f"Found {len(all_items)} total Ethics articles, {len(book_review_dois)} book reviews")
        return book_review_dois
    
    def upload_to_db(self, records: List[Dict]) -> bool:
        """Insert records into the local SQLite database, skipping duplicates."""
        if not records:
            return True

        filtered = []
        for record in records:
            doi = record.get('DOI')
            if doi and not db.doi_exists(doi):
                flag_reason = record.pop('_flag_reason', None)
                filtered.append(record)

                entry_summary = {
                    'doi': doi,
                    'title': record.get('Book Title', ''),
                    'author': f"{record.get('Book Author First Name', '')} {record.get('Book Author Last Name', '')}".strip(),
                    'reviewer': f"{record.get('Reviewer First Name', '')} {record.get('Reviewer Last Name', '')}".strip(),
                    'date': record.get('Publication Date', ''),
                    'link': record.get('Review Link', '')
                }
                self.uploaded_entries.append(entry_summary)

                if flag_reason:
                    entry_summary['flag'] = flag_reason
            elif doi:
                self.log(f"Skipping DOI {doi} - already exists in database")

        if not filtered:
            return False

        # Convert Airtable-style field names to snake_case DB columns
        db_records = [_to_db_fields(r) for r in filtered]
        db.insert_reviews(db_records)
        self.stats['db_inserts'] += len(db_records)
        return True
    
    def process_dois_batch(self, dois: List[str]) -> List[Dict]:
        results = []
        
        for i, doi in enumerate(dois):
            self.stats['dois_processed'] += 1
            crossref_data = self.get_crossref_metadata(doi)
            
            if crossref_data:
                if self.is_book_review(crossref_data):
                    review_data = self.extract_review_data(crossref_data)
                    
                    has_data = (review_data.get('Book Title') and 
                               review_data.get('Book Author Last Name') and 
                               review_data.get('Reviewer Last Name'))
                    
                    if has_data:
                        if not self.validate_title(review_data.get('Book Title', '')):
                            self.flag_entry(review_data, 'Invalid title')
                        elif len(review_data.get('Book Author Last Name', '')) == 1:
                            self.flag_entry(review_data, 'Single letter last name')
                        
                        results.append(review_data)
                        self.stats['successful_extractions'] += 1
            
            time.sleep(0.5)
        
        return results
    
    def generate_report(self, output_file: str = "philreviews_report.md"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        report = f"""# PhilReviews Scraper Report
Generated: {timestamp}

## Summary Statistics
- **DOIs Processed:** {self.stats['dois_processed']}
- **Successful Extractions:** {self.stats['successful_extractions']}
- **New Uploads:** {self.stats['db_inserts']}
- **Flagged Entries:** {self.stats['flagged_entries']}
- **Errors:** {self.stats['errors']}
- **Success Rate:** {(self.stats['successful_extractions'] / self.stats['dois_processed'] * 100) if self.stats['dois_processed'] > 0 else 0:.1f}%

"""
        
        if self.uploaded_entries:
            report += f"\n## Newly Added Entries ({len(self.uploaded_entries)})\n\n"
            for entry in self.uploaded_entries:
                flag = entry.get('flag', '')
                flag_marker = ' FLAG' if flag else ''
                report += f"- **{entry['title']}**{flag_marker}\n"
                report += f"  - Author: {entry['author']}\n"
                report += f"  - Reviewer: {entry['reviewer']}\n"
                report += f"  - [DOI]({entry['link']})\n\n"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        self.log(f"Report generated: {output_file}")
        return output_file
    
    def run_full_pipeline(self, max_reviews: int = 0):
        self.stats['start_time'] = datetime.now()

        self.log("Starting PhilReviews scraper pipeline")
        self.log(f"Fetching Ethics book reviews (limit: {'all' if not max_reviews else max_reviews})")

        try:
            dois = self.search_ethics_dois(max_items=0)

            if not dois:
                self.log("No DOIs found. Pipeline stopped.", "ERROR")
                return

            if max_reviews:
                dois = dois[:max_reviews]
            results = self.process_dois_batch(dois)
            
            if results:
                self.upload_to_db(results)
            
            self.print_final_stats()
            self.generate_report()
            
        except Exception as e:
            self.log(f"Pipeline failed: {e}", "ERROR")
            raise
    
    def print_final_stats(self):
        if self.stats['start_time']:
            duration = datetime.now() - self.stats['start_time']
            duration_str = str(duration).split('.')[0]
        else:
            duration_str = "Unknown"
        
        self.log("\nPIPELINE STATISTICS")
        self.log("=" * 30)
        self.log(f"Total Runtime: {duration_str}")
        self.log(f"DOIs Processed: {self.stats['dois_processed']}")
        self.log(f"Successful Extractions: {self.stats['successful_extractions']}")
        self.log(f"DB Inserts: {self.stats['db_inserts']}")
        self.log(f"Flagged Entries: {self.stats['flagged_entries']}")
        self.log(f"Errors: {self.stats['errors']}")
        
        if self.stats['dois_processed'] > 0:
            success_rate = (self.stats['successful_extractions'] / self.stats['dois_processed']) * 100
            self.log(f"Success Rate: {success_rate:.1f}%")

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
    
    parser = argparse.ArgumentParser(description='PhilReviews - Ethics Journal Scraper')
    parser.add_argument('--max-reviews', type=int, default=0,
                        help='Max reviews to process (0 = all)')
    parser.add_argument('--test-dois', nargs='+', help='Test with specific DOIs')

    args = parser.parse_args()

    try:
        scraper = PhilReviewsScraper()

        if args.test_dois:
            results = scraper.process_dois_batch(args.test_dois)
            scraper.upload_to_db(results)
            scraper.print_final_stats()
        else:
            scraper.run_full_pipeline(max_reviews=args.max_reviews)
            
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
