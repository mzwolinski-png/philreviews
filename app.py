import json

from flask import Flask, render_template

import db

app = Flask(__name__)


def normalize(record):
    author = " ".join(
        filter(None, [record.get("book_author_first_name", ""),
                      record.get("book_author_last_name", "")])
    )
    reviewer = " ".join(
        filter(None, [record.get("reviewer_first_name", ""),
                      record.get("reviewer_last_name", "")])
    )
    link = record.get("review_link") or ""
    if not link and record.get("doi"):
        link = f"https://doi.org/{record['doi']}"

    return {
        "title": record.get("book_title", ""),
        "author": author,
        "reviewer": reviewer,
        "journal": record.get("publication_source", ""),
        "date": record.get("publication_date", ""),
        "link": link,
        "summary": record.get("review_summary", ""),
        "access": record.get("access_type", ""),
    }


@app.route("/")
def index():
    records = db.get_all_reviews()
    reviews = [normalize(r) for r in records]

    journals = sorted({r["journal"] for r in reviews if r["journal"]})
    years = [int(r["date"][:4]) for r in reviews if r["date"] and len(r["date"]) >= 4 and r["date"][:4].isdigit()]
    min_year = min(years) if years else 2000
    max_year = max(years) if years else 2026

    # Source counts for the sources section
    from collections import Counter
    source_counts = Counter(r["journal"] for r in reviews if r["journal"])
    sources = sorted(source_counts.items(), key=lambda x: -x[1])

    return render_template(
        "index.html",
        reviews_json=json.dumps(reviews, ensure_ascii=False),
        total=len(reviews),
        journals=journals,
        sources=sources,
        min_year=min_year,
        max_year=max_year,
    )


if __name__ == "__main__":
    app.run(debug=True)
