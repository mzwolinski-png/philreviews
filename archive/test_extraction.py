#!/usr/bin/env python3
"""
Test the NDPR extraction module against real NDPR pages.
Fetches sample pages and validates that extraction produces correct results.
"""

import requests
from bs4 import BeautifulSoup
from ndpr_extraction import extract_review_data, is_review_page, is_valid_review_url, split_name, parse_author_string, parse_reviewer_string

SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'PhilReviews/2.0 (test suite)'})


def fetch(url):
    resp = SESSION.get(url, timeout=15)
    resp.raise_for_status()
    return BeautifulSoup(resp.content, 'html.parser')


def test_live_pages():
    """Test extraction against real NDPR review pages."""

    test_cases = [
        {
            'url': 'https://ndpr.nd.edu/reviews/analytic-philosophy-and-human-life/',
            'expect': {
                'book_title': 'Analytic Philosophy and Human Life',
                'book_author_first': 'Thomas',
                'book_author_last': 'Nagel',
                'reviewer_first': 'A.W.',
                'reviewer_last': 'Moore',
            }
        },
        {
            'url': 'https://ndpr.nd.edu/reviews/humes-imagination/',
            'expect': {
                'book_title': "Hume's Imagination",
                'book_author_first': 'Tito',
                'book_author_last': 'Magri',
                'reviewer_first': 'Donald C.',
                'reviewer_last': 'Ainslie',
            }
        },
        {
            'url': 'https://ndpr.nd.edu/reviews/current-controversies-in-philosophy-of-mind/',
            'expect': {
                'book_title': 'Current Controversies in Philosophy of Mind',
                'book_author_first': 'Uriah',
                'book_author_last': 'Kriegel',
                '_metadata.is_edited_volume': True,
            }
        },
        {
            'url': 'https://ndpr.nd.edu/reviews/writing-the-book-of-the-world/',
            'expect': {
                'book_title': 'Writing the Book of the World',
                'book_author_first': 'Theodore',
                'book_author_last': 'Sider',
                '_metadata.has_multiple_reviewers': True,
            }
        },
        {
            'url': 'https://ndpr.nd.edu/reviews/korean-women-philosophers-and-the-ideal-of-a-female-sage-essential-writings-of-im-yungjidang-and-gang-jeongildang/',
            'expect': {
                'book_author_first': 'Philip J.',
                'book_author_last': 'Ivanhoe',
                '_metadata.has_multiple_authors': True,
                'reviewer_first': 'Erin M.',
                'reviewer_last': 'Cline',
            }
        },
        {
            'url': 'https://ndpr.nd.edu/reviews/beyond-words-philosophy-fiction-and-the-unsayable/',
            'expect': {
                'book_author_first': 'Timothy',
                'book_author_last': 'Cleveland',
                'reviewer_last': 'Jovanović',
            }
        },
        {
            'url': 'https://ndpr.nd.edu/reviews/plato/',
            'expect': {
                'book_title': 'Plato',
                'book_author_first': 'Andrew S.',
                'book_author_last': 'Mason',
                'reviewer_first': 'Frisbee C. C.',
                'reviewer_last': 'Sheffield',
            }
        },
    ]

    passed = 0
    failed = 0

    for tc in test_cases:
        url = tc['url']
        slug = url.rstrip('/').split('/')[-1]
        print(f"\n--- {slug} ---")

        try:
            soup = fetch(url)
        except Exception as e:
            print(f"  FETCH ERROR: {e}")
            failed += 1
            continue

        if not is_review_page(soup):
            print(f"  FAIL: not detected as review page")
            failed += 1
            continue

        data = extract_review_data(soup, url)
        if not data:
            print(f"  FAIL: extract_review_data returned None")
            failed += 1
            continue

        case_ok = True
        for key, expected in tc['expect'].items():
            if key.startswith('_metadata.'):
                meta_key = key.split('.', 1)[1]
                actual = data.get('_metadata', {}).get(meta_key)
            else:
                actual = data.get(key)

            if actual != expected:
                print(f"  FAIL: {key} = {actual!r}, expected {expected!r}")
                case_ok = False

        # Check that we always get a publication date
        if not data.get('publication_date'):
            print(f"  WARN: no publication_date extracted")

        if case_ok:
            print(f"  PASS: {data.get('book_title', '?')}")
            print(f"    Author: {data.get('book_author_first', '')} {data.get('book_author_last', '')}")
            print(f"    Reviewer: {data.get('reviewer_first', '')} {data.get('reviewer_last', '')}")
            print(f"    Date: {data.get('publication_date', '?')}")
            passed += 1
        else:
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed out of {len(test_cases)} tests")
    return failed == 0


def test_non_review_pages():
    """Test that non-review pages are correctly rejected."""
    print("\n--- Non-review page tests ---")

    non_review_urls = [
        'https://ndpr.nd.edu/reviews/archives/2025/',
        'https://ndpr.nd.edu/',
    ]

    passed = 0
    for url in non_review_urls:
        try:
            soup = fetch(url)
            if is_review_page(soup):
                print(f"  FAIL: {url} incorrectly identified as review")
            else:
                print(f"  PASS: {url} correctly rejected")
                passed += 1
        except Exception as e:
            print(f"  ERROR fetching {url}: {e}")

    # Test 404 URLs
    test_404 = 'https://ndpr.nd.edu/reviews/a-commentary/'
    try:
        resp = SESSION.get(test_404, timeout=10)
        if resp.status_code == 404:
            print(f"  PASS: {test_404} returns 404")
            passed += 1
        else:
            print(f"  INFO: {test_404} returned {resp.status_code}")
    except Exception as e:
        print(f"  ERROR: {e}")

    return passed


def test_url_validation():
    """Test the URL validation function."""
    print("\n--- URL validation tests ---")

    valid = [
        'https://ndpr.nd.edu/reviews/analytic-philosophy-and-human-life/',
        'https://ndpr.nd.edu/reviews/plato/',
    ]
    invalid = [
        'https://ndpr.nd.edu/reviews/',
        'https://ndpr.nd.edu/reviews/archives/2025/',
        'https://ndpr.nd.edu/reviews/archives/2003/',
        'https://ndpr.nd.edu/about/',
        '',
    ]

    passed = 0
    for url in valid:
        if is_valid_review_url(url):
            print(f"  PASS: {url} accepted")
            passed += 1
        else:
            print(f"  FAIL: {url} should be valid")

    for url in invalid:
        if not is_valid_review_url(url):
            print(f"  PASS: {url!r} rejected")
            passed += 1
        else:
            print(f"  FAIL: {url!r} should be invalid")

    return passed


def test_name_splitting():
    """Test the name splitting logic."""
    print("\n--- Name splitting tests ---")

    cases = [
        ("Thomas Nagel", ("Thomas", "Nagel")),
        ("A.W. Moore", ("A.W.", "Moore")),
        ("Donald C. Ainslie", ("Donald C.", "Ainslie")),
        ("Frisbee C. C. Sheffield", ("Frisbee C. C.", "Sheffield")),
        ("Jean-Paul Sartre", ("Jean-Paul", "Sartre")),
        ("Philip J. Ivanhoe", ("Philip J.", "Ivanhoe")),
        ("Nagel", ("", "Nagel")),
        ("", ("", "")),
    ]

    passed = 0
    for name, expected in cases:
        result = split_name(name)
        if result == expected:
            print(f"  PASS: {name!r} -> {result}")
            passed += 1
        else:
            print(f"  FAIL: {name!r} -> {result}, expected {expected}")

    return passed


def test_author_parsing():
    """Test bibliography author string parsing."""
    print("\n--- Author string parsing tests ---")

    cases = [
        ("Thomas Nagel", {'first': 'Thomas', 'last': 'Nagel', 'is_edited': False, 'has_multiple': False}),
        ("Uriah Kriegel (ed.)", {'first': 'Uriah', 'last': 'Kriegel', 'is_edited': True, 'has_multiple': False}),
        ("Philip J. Ivanhoe and Hwa Yeong Wang", {'first': 'Philip J.', 'last': 'Ivanhoe', 'is_edited': False, 'has_multiple': True}),
        ("John Smith and Jane Doe (eds.)", {'first': 'John', 'last': 'Smith', 'is_edited': True, 'has_multiple': True}),
    ]

    passed = 0
    for author_str, expected in cases:
        result = parse_author_string(author_str)
        if result == expected:
            print(f"  PASS: {author_str!r}")
            passed += 1
        else:
            print(f"  FAIL: {author_str!r}")
            print(f"    got:      {result}")
            print(f"    expected: {expected}")

    return passed


def test_reviewer_parsing():
    """Test reviewer string parsing."""
    print("\n--- Reviewer string parsing tests ---")

    cases = [
        ("A.W. Moore, University of Oxford",
         {'first': 'A.W.', 'last': 'Moore', 'affiliation': 'University of Oxford', 'has_multiple': False}),
        ("Timothy O'Connor and Nickolas Montgomery, Indiana University",
         {'first': "Timothy", 'last': "O'Connor", 'affiliation': 'Indiana University', 'has_multiple': True}),
        ("Frisbee C. C. Sheffield, King's College London",
         {'first': 'Frisbee C. C.', 'last': 'Sheffield', 'affiliation': "King's College London", 'has_multiple': False}),
    ]

    passed = 0
    for reviewer_str, expected in cases:
        result = parse_reviewer_string(reviewer_str)
        if result == expected:
            print(f"  PASS: {reviewer_str!r}")
            passed += 1
        else:
            print(f"  FAIL: {reviewer_str!r}")
            print(f"    got:      {result}")
            print(f"    expected: {expected}")

    return passed


if __name__ == '__main__':
    print("NDPR Extraction Module Tests")
    print("=" * 50)

    test_name_splitting()
    test_author_parsing()
    test_reviewer_parsing()
    test_url_validation()
    test_non_review_pages()
    test_live_pages()
