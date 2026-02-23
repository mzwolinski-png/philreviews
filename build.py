#!/usr/bin/env python3
"""
Build the PhilReviews static site into docs/ for GitHub Pages.

Renders the Flask app to static HTML and copies assets so the site
can be served without a backend.
"""

import csv
import os
import shutil
import sys

# Ensure imports work regardless of cwd
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

DOCS_DIR = os.path.join(ROOT, "docs")
STATIC_SRC = os.path.join(ROOT, "static")
STATIC_DST = os.path.join(DOCS_DIR, "static")


def build():
    from app import app

    # Render pages via Flask's test client
    with app.test_client() as client:
        resp = client.get("/")
        html = resp.data.decode("utf-8")

        resp_cl = client.get("/changelog")
        changelog_html = resp_cl.data.decode("utf-8")

    # Prepare docs/ directory
    os.makedirs(STATIC_DST, exist_ok=True)

    # Rewrite absolute paths to relative so it works under a subpath
    # e.g. /static/style.css → static/style.css
    html = html.replace('"/static/', '"static/')
    html = html.replace("'/static/", "'static/")
    changelog_html = changelog_html.replace('"/static/', '"static/')
    changelog_html = changelog_html.replace("'/static/", "'static/")

    # Write HTML
    index_path = os.path.join(DOCS_DIR, "index.html")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html)

    changelog_path = os.path.join(DOCS_DIR, "changelog.html")
    with open(changelog_path, "w", encoding="utf-8") as f:
        f.write(changelog_html)

    # Copy static assets
    for fname in os.listdir(STATIC_SRC):
        src = os.path.join(STATIC_SRC, fname)
        if os.path.isfile(src):
            shutil.copy2(src, STATIC_DST)

    # Generate CSV data export
    from db import get_all_reviews
    reviews = get_all_reviews()
    csv_path = os.path.join(DOCS_DIR, "philreviews.csv")
    fieldnames = [
        "title", "author", "reviewer", "journal", "date",
        "link", "doi", "access", "type",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in reviews:
            first = (r.get("book_author_first_name") or "").strip()
            last = (r.get("book_author_last_name") or "").strip()
            author = f"{first} {last}".strip()
            rev_first = (r.get("reviewer_first_name") or "").strip()
            rev_last = (r.get("reviewer_last_name") or "").strip()
            reviewer = f"{rev_first} {rev_last}".strip()
            writer.writerow({
                "title": r.get("book_title", ""),
                "author": author,
                "reviewer": reviewer,
                "journal": r.get("publication_source", ""),
                "date": r.get("publication_date", ""),
                "link": r.get("review_link", ""),
                "doi": r.get("doi", ""),
                "access": r.get("access_type", ""),
                "type": r.get("entry_type", ""),
            })

    # Copy favicon to docs root so /favicon.svg works
    favicon_src = os.path.join(STATIC_SRC, "favicon.svg")
    if os.path.exists(favicon_src):
        shutil.copy2(favicon_src, os.path.join(DOCS_DIR, "favicon.svg"))

    # Write CNAME for custom domain
    with open(os.path.join(DOCS_DIR, "CNAME"), "w") as f:
        f.write("philreviews.org")

    # Print summary
    html_size = os.path.getsize(index_path)
    cl_size = os.path.getsize(changelog_path)
    csv_size = os.path.getsize(csv_path)
    print(f"Built docs/index.html ({html_size:,} bytes)")
    print(f"Built docs/changelog.html ({cl_size:,} bytes)")
    print(f"Built docs/philreviews.csv ({csv_size:,} bytes)")
    for fname in sorted(os.listdir(STATIC_DST)):
        fpath = os.path.join(STATIC_DST, fname)
        print(f"  docs/static/{fname} ({os.path.getsize(fpath):,} bytes)")
    print("Static site ready in docs/")


if __name__ == "__main__":
    build()
