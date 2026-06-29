import json
import os
import re
import secrets
import threading
import time
from collections import defaultdict, deque
from urllib.parse import quote, quote_plus

from flask import Flask, render_template, request, jsonify, make_response, abort, redirect
from markupsafe import escape

import db

app = Flask(__name__)


# --- Abuse protection for the email-sending /subscribe endpoint ------------
# Single gunicorn worker (see Dockerfile), so a per-process in-memory limiter
# is sufficient. Guards against using the verification-email sender as an open
# relay to spam arbitrary addresses / burn the Gmail send quota.
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _valid_email(addr):
    """Strict-enough validation that also rejects CRLF (header-injection)."""
    return (
        bool(addr) and len(addr) <= 254
        and "\r" not in addr and "\n" not in addr
        and bool(_EMAIL_RE.match(addr))
    )


class _RateLimiter:
    """Thread-safe in-memory sliding-window rate limiter."""

    def __init__(self):
        self._hits = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(self, key, limit, window_s):
        now = time.time()
        with self._lock:
            dq = self._hits[key]
            while dq and dq[0] <= now - window_s:
                dq.popleft()
            if len(dq) >= limit:
                return False
            dq.append(now)
            if len(self._hits) > 10000:  # bound memory
                for k in [k for k, d in self._hits.items()
                          if not d or d[-1] <= now - 86400]:
                    del self._hits[k]
            return True


_rate_limiter = _RateLimiter()


def _client_ip():
    """Real client IP behind Fly's proxy (X-Forwarded-For), else remote_addr."""
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or "unknown"

SUBFIELD_NAMES = {
    "ethics": "Ethics & Moral Philosophy",
    "applied-ethics": "Applied & Professional Ethics",
    "political": "Political & Social Philosophy",
    "legal": "Philosophy of Law",
    "epistemology": "Epistemology & Philosophy of Mind",
    "metaphysics": "Metaphysics & Logic",
    "science": "Philosophy of Science",
    "aesthetics": "Aesthetics & Philosophy of Art",
    "religion": "Philosophy of Religion & Theology",
    "history": "History of Philosophy",
    "ancient": "Ancient & Medieval Philosophy",
    "modern": "Early Modern Philosophy",
    "continental": "Continental & Phenomenological",
    "feminist": "Feminist Philosophy",
    "non-western": "Non-Western & Comparative",
}

BASE_URL = "https://philreviews.org"


@app.context_processor
def inject_globals():
    return {"SUBFIELD_NAMES": SUBFIELD_NAMES, "BASE_URL": BASE_URL}


@app.template_filter("strip_the")
def strip_the_filter(s):
    """Remove leading 'The ' from journal names for display."""
    return s[4:] if s.startswith("The ") else s


@app.template_filter("url_encode")
def url_encode_filter(s):
    return quote(s, safe="")


def _search_link(title, reviewer, author, journal):
    """A public 'find this review' Google search, used in place of links that
    resolve to a subscription database the public can't reach."""
    parts = []
    if title:
        parts.append('"%s"' % title)
    parts.append(reviewer or author)
    if journal:
        parts.append(journal)
    if not reviewer:
        parts.append("review")
    q = " ".join(p for p in parts if p).strip()
    return "https://www.google.com/search?q=" + quote_plus(q)


def normalize(record):
    """Convert a DB row dict to the API response format."""
    author = " ".join(
        filter(None, [record.get("book_author_first_name", ""),
                      record.get("book_author_last_name", "")])
    )
    reviewer = " ".join(
        filter(None, [record.get("reviewer_first_name", ""),
                      record.get("reviewer_last_name", "")])
    )
    link = record.get("review_link") or ""
    link_search = False
    if link and "ebsco.com" in link:
        # These links resolve to a subscription database (no public access).
        # Serve a Google search for the review instead — never expose the
        # original link in the UI or the raw API.
        link = _search_link(record.get("book_title", ""), reviewer, author,
                            record.get("publication_source", ""))
        link_search = True
    elif not link and record.get("doi"):
        link = f"https://doi.org/{record['doi']}"

    return {
        "id": record.get("id"),
        "title": record.get("book_title", ""),
        "author": author,
        "reviewer": reviewer,
        "journal": record.get("publication_source", ""),
        "date": record.get("publication_date", ""),
        "link": link,
        "link_search": link_search,
        "summary": record.get("review_summary", ""),
        "access": record.get("access_type", ""),
        "type": record.get("entry_type", "review") or "review",
        "symposium_group": record.get("symposium_group", ""),
        "subfield": record.get("subfield_primary") or "",
        "subfield2": record.get("subfield_secondary") or "",
    }


# --- Main pages ---

@app.route("/")
def index():
    meta = db.get_metadata()
    # Server-render the first page of recent reviews so the homepage has
    # crawlable, no-JS-usable content (the JS re-renders on load).
    result = db.search_reviews(sort="date", sort_dir="desc", page=1, per_page=25)
    initial_reviews = [normalize(r) for r in result["reviews"]]
    return render_template(
        "index.html",
        total=meta["total"],
        journal_count=len(meta["journals"]),
        min_year=meta["min_year"],
        max_year=meta["max_year"],
        initial_reviews=initial_reviews,
    )


@app.route("/changelog")
def changelog():
    return render_template("changelog.html")


# --- Redirects for old GitHub Pages URLs ---

@app.route("/index.html")
@app.route("/docs/")
@app.route("/docs/index.html")
def old_index():
    return redirect("/", code=301)


@app.route("/docs/changelog.html")
def old_changelog():
    return redirect("/changelog", code=301)


@app.route("/docs/static/<path:filename>")
def old_static(filename):
    return redirect(f"/static/{filename}", code=301)


# --- Subscription endpoints ---

@app.route("/subscribe", methods=["POST"])
def subscribe():
    email = (request.form.get("email") or "").strip().lower()
    if not _valid_email(email):
        return jsonify({"error": "Valid email required"}), 400

    # Rate-limit before generating a token / sending mail: max 5 attempts per IP
    # per hour, and per address max 2 verification emails/hour and 5/day.
    ip = _client_ip()
    if not _rate_limiter.allow(f"sub_ip:{ip}", 5, 3600):
        return jsonify({"error": "Too many requests. Please try again later."}), 429
    if not _rate_limiter.allow(f"sub_email_hr:{email}", 2, 3600) \
            or not _rate_limiter.allow(f"sub_email_day:{email}", 5, 86400):
        return jsonify({"error": "Too many requests for this address. Please try again later."}), 429

    selected = request.form.getlist("subfields")
    if not selected or "all" in selected:
        subfields = "all"
    else:
        valid = set(SUBFIELD_NAMES.keys())
        subfields = ",".join(s for s in selected if s in valid)
        if not subfields:
            subfields = "all"

    token = secrets.token_urlsafe(32)
    db.add_subscriber(email, subfields, token)

    # Send verification email
    try:
        from notify import send_verification_email
        send_verification_email(email, token)
    except Exception:
        pass  # Email may fail on server (no SMTP creds) — subscriber is still saved

    return jsonify({"ok": True, "message": "Check your email to confirm your subscription. (Check your spam folder if you don't see it, and add updates@philreviews.org to your contacts.)"})


@app.route("/verify")
def verify():
    token = request.args.get("token", "")
    if token and db.verify_subscriber(token):
        return render_template("subscribe_ok.html", action="verified")
    abort(404)


@app.route("/unsubscribe")
def unsubscribe():
    token = request.args.get("token", "")
    if token and db.unsubscribe(token):
        return render_template("subscribe_ok.html", action="unsubscribed")
    abort(404)


# --- SEO pages ---

@app.route("/journal/<path:name>")
def journal_page(name):
    page = max(1, request.args.get("page", 1, type=int))
    result = db.get_journal_reviews(name, page=page, per_page=50)
    if result is None:
        abort(404)
    reviews = [normalize(r) for r in result["reviews"]]
    resp = make_response(render_template(
        "journal.html",
        journal_name=name,
        reviews=reviews,
        total=result["total"],
        page=result["page"],
        total_pages=result["total_pages"],
        min_year=result["min_year"],
        max_year=result["max_year"],
    ))
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


@app.route("/subfield/<code>")
def subfield_page(code):
    if code not in SUBFIELD_NAMES:
        abort(404)
    page = max(1, request.args.get("page", 1, type=int))
    result = db.get_subfield_reviews(code, page=page, per_page=50)
    if result is None:
        abort(404)
    reviews = [normalize(r) for r in result["reviews"]]
    # Top journals for this subfield
    meta = db.get_metadata()
    top_journals = []
    for j in meta["journals"]:
        if len(top_journals) >= 10:
            break
        top_journals.append(j)
    resp = make_response(render_template(
        "subfield.html",
        subfield_code=code,
        subfield_name=SUBFIELD_NAMES[code],
        reviews=reviews,
        total=result["total"],
        page=result["page"],
        total_pages=result["total_pages"],
        top_journals=top_journals,
    ))
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


@app.route("/journals")
def journals_index():
    meta = db.get_metadata()
    journals = sorted(meta["journals"], key=lambda j: j["name"].replace("The ", "").lower())
    resp = make_response(render_template("journals_index.html", journals=journals))
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


@app.route("/subfields")
def subfields_index():
    meta = db.get_metadata()
    subfields = meta["subfields"]
    resp = make_response(render_template("subfields_index.html", subfields=subfields))
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


@app.route("/robots.txt")
def robots_txt():
    resp = make_response("User-agent: *\nAllow: /\n\nSitemap: https://philreviews.org/sitemap.xml\n")
    resp.headers["Content-Type"] = "text/plain"
    return resp


@app.route("/sitemap.xml")
def sitemap():
    meta = db.get_metadata()
    xml = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    # Homepage
    xml.append(f'<url><loc>{BASE_URL}/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>')
    # Index pages
    xml.append(f'<url><loc>{BASE_URL}/journals</loc><changefreq>weekly</changefreq><priority>0.8</priority></url>')
    xml.append(f'<url><loc>{BASE_URL}/subfields</loc><changefreq>weekly</changefreq><priority>0.8</priority></url>')
    xml.append(f'<url><loc>{BASE_URL}/changelog</loc><changefreq>weekly</changefreq><priority>0.3</priority></url>')
    # Subfield pages
    for code in SUBFIELD_NAMES:
        xml.append(f'<url><loc>{BASE_URL}/subfield/{escape(code)}</loc><changefreq>weekly</changefreq><priority>0.7</priority></url>')
    # Journal pages
    for j in meta["journals"]:
        encoded = quote(j["name"], safe="")
        xml.append(f'<url><loc>{BASE_URL}/journal/{encoded}</loc><changefreq>weekly</changefreq><priority>0.6</priority></url>')
    xml.append('</urlset>')
    resp = make_response("\n".join(xml))
    resp.headers["Content-Type"] = "application/xml"
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


# --- API ---

@app.route("/api/reviews")
def api_reviews():
    q = request.args.get("q", "").strip() or None
    title = request.args.get("title", "").strip() or None
    author = request.args.get("author", "").strip() or None
    reviewer = request.args.get("reviewer", "").strip() or None

    journals_str = request.args.get("journals", "").strip()
    journals = [j for j in journals_str.split(",") if j] if journals_str else None

    subfield_str = request.args.get("subfield", "").strip()
    subfields = [s for s in subfield_str.split(",") if s] if subfield_str else None

    year_from = request.args.get("year_from", type=int)
    year_to = request.args.get("year_to", type=int)
    if year_from is not None:
        year_from = max(1800, min(year_from, 2100))
    if year_to is not None:
        year_to = max(1800, min(year_to, 2100))
    access = request.args.get("access", "").strip() or None
    entry_type = request.args.get("type", "").strip() or None

    sort = request.args.get("sort", "date")
    if sort not in ("date", "title", "author", "reviewer", "journal"):
        sort = "date"
    sort_dir = request.args.get("sort_dir", "desc")
    if sort_dir not in ("asc", "desc"):
        sort_dir = "desc"
    page = max(1, request.args.get("page", 1, type=int))
    per_page = max(1, min(request.args.get("per_page", 25, type=int), 250))

    result = db.search_reviews(
        q=q, title=title, author=author, reviewer=reviewer,
        journals=journals, subfields=subfields,
        year_from=year_from, year_to=year_to,
        access=access, entry_type=entry_type,
        sort=sort, sort_dir=sort_dir, page=page, per_page=per_page,
    )

    reviews = [normalize(r) for r in result["reviews"]]

    sym_groups = {}
    for rev in reviews:
        if rev["type"] == "symposium" and rev["symposium_group"]:
            sym_groups[rev["symposium_group"]] = rev["id"]
    if sym_groups:
        all_peers = db.get_symposium_peers_batch(sym_groups)
        for rev in reviews:
            grp = rev.get("symposium_group")
            if grp and grp in all_peers:
                rev["peers"] = [normalize(p) for p in all_peers[grp]]

    resp = jsonify({
        "reviews": reviews,
        "total": result["total"],
        "page": result["page"],
        "per_page": result["per_page"],
        "total_pages": result["total_pages"],
    })
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.route("/api/metadata")
def api_metadata():
    meta = db.get_metadata()
    resp = jsonify(meta)
    resp.headers["Cache-Control"] = "public, max-age=600"
    return resp


@app.route("/health")
def health():
    try:
        count = db._get_total_count()
        try:
            subs = db.get_verified_subscriber_count()
        except Exception:
            subs = None
        return jsonify({"status": "ok", "reviews": count, "subscribers": subs}), 200
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 503


@app.after_request
def add_cache_headers(response):
    if request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=86400"
    return response


if __name__ == "__main__":
    app.run(debug=os.environ.get("FLASK_DEBUG") == "1")
