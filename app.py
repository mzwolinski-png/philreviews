import json
import os
import re
import secrets
from datetime import datetime
import threading
import time
from collections import defaultdict, deque
from urllib.parse import quote, quote_plus

from flask import Flask, render_template, request, jsonify, make_response, abort, redirect, url_for
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

def _looks_automated(form):
    """Cheap bot checks for the signup forms.

    A flood of ~67 signups/day in Aug 2026 — none ever verified, mostly Gmail
    addresses with dots sprinkled through the local part — burned 80% of the
    daily Resend quota on verification emails nobody asked for. Both forms
    already submit via fetch(), so requiring JS costs real users nothing.

    Returns a short reason string if the submission looks automated, else "".
    """
    # 1. Honeypot: a field hidden from humans; only a bot fills it in.
    if (form.get("website") or "").strip():
        return "honeypot"
    # 2. Proof the page ran our JS and a human paused over the form.
    ts = (form.get("ts") or "").strip()
    if not ts.isdigit():
        return "no-js"
    elapsed = time.time() * 1000 - int(ts)
    if elapsed < 2000:
        return "too-fast"
    if elapsed > 6 * 3600 * 1000:
        return "stale-form"
    return ""


# Backstop so a flood can never exhaust the mail quota again. Resend's free
# tier allows 100/day; the digest needs ~25 of those once a week.
_VERIFY_SEND_CAP_PER_DAY = 40


@app.route("/subscribe", methods=["POST"])
def subscribe():
    email = (request.form.get("email") or "").strip().lower()
    if not _valid_email(email):
        return jsonify({"error": "Valid email required"}), 400

    bot = _looks_automated(request.form)
    if bot:
        app.logger.info("subscribe rejected (%s): %s", bot, email)
        # Same wording as success — don't teach a bot which check caught it.
        return jsonify({"ok": True, "message":
                        "Check your email to confirm your subscription."})

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

    # Send verification email (subject to the daily quota backstop)
    if _rate_limiter.allow("verify_sends_day", _VERIFY_SEND_CAP_PER_DAY, 86400):
        try:
            from notify import send_verification_email
            send_verification_email(email, token)
        except Exception:
            pass  # Email may fail on server (no SMTP creds) — subscriber is still saved
    else:
        app.logger.warning("daily verification-send cap reached; skipped %s", email)

    return jsonify({"ok": True, "message": "Check your email to confirm your subscription. (Check your spam folder if you don't see it, and add updates@philreviews.org to your contacts.)"})


@app.route("/verify", methods=["GET", "POST"])
def verify():
    """Two-step confirmation. A GET only renders a page with a confirm button;
    the subscription is activated solely by the POST that button submits.

    Why: corporate mail-security scanners fetch every link in an inbound email,
    which silently completed the old one-click GET flow and produced 'verified'
    subscribers who never opted in (found 2026-08-04 — e.g. addresses at
    valvesoftware.com, wsjbuilding.com). Scanners follow links; they do not
    submit forms.
    """
    token = (request.form.get("token") if request.method == "POST"
             else request.args.get("token", "")) or ""
    if not token:
        abort(404)
    if request.method == "GET":
        if not db.pending_token_exists(token, "subscriber"):
            abort(404)
        return render_template("confirm.html", token=token, kind="subscription",
                               action_url=url_for("verify"))
    if db.verify_subscriber(token):
        return render_template("subscribe_ok.html", action="verified")
    abort(404)


@app.route("/unsubscribe")
def unsubscribe():
    token = request.args.get("token", "")
    if token and db.unsubscribe(token):
        return render_template("subscribe_ok.html", action="unsubscribed")
    abort(404)


@app.route("/follow", methods=["POST"])
def follow():
    """Register an alert for new reviews of a specific book or author."""
    email = (request.form.get("email") or "").strip().lower()
    ftype = (request.form.get("type") or "").strip()
    value = (request.form.get("value") or "").strip()
    if not _valid_email(email):
        return jsonify({"error": "Valid email required"}), 400
    if ftype not in ("book", "author") or not (2 < len(value) < 300):
        return jsonify({"error": "Invalid follow request"}), 400

    bot = _looks_automated(request.form)
    if bot:
        app.logger.info("follow rejected (%s): %s", bot, email)
        return jsonify({"ok": True, "message": "Check your email to confirm this alert."})

    ip = _client_ip()
    if not _rate_limiter.allow(f"fol_ip:{ip}", 10, 3600):
        return jsonify({"error": "Too many requests. Please try again later."}), 429
    if not _rate_limiter.allow(f"fol_email_hr:{email}", 5, 3600) \
            or not _rate_limiter.allow(f"fol_email_day:{email}", 15, 86400):
        return jsonify({"error": "Too many requests for this address. Please try again later."}), 429

    token = secrets.token_urlsafe(32)
    status = db.add_follow(email, ftype, value, token)
    what = f"reviews of “{value}”" if ftype == "book" else f"new reviews of books by {value}"
    if status == "exists":
        return jsonify({"ok": True, "message": f"You're already following {what}."})
    if status == "active":
        return jsonify({"ok": True, "message":
            f"Done — you'll get an email when we index {what}."})
    if _rate_limiter.allow("verify_sends_day", _VERIFY_SEND_CAP_PER_DAY, 86400):
        try:
            from notify import send_follow_verification_email
            send_follow_verification_email(email, token, what)
        except Exception:
            pass
    else:
        app.logger.warning("daily verification-send cap reached; skipped follow %s", email)
    return jsonify({"ok": True, "message":
        "Check your email to confirm this alert. (Check spam if you don't see "
        "it, and add updates@philreviews.org to your contacts.)"})


@app.route("/follow/verify", methods=["GET", "POST"])
def follow_verify():
    """Same two-step confirmation as /verify — see the note there."""
    token = (request.form.get("token") if request.method == "POST"
             else request.args.get("token", "")) or ""
    if not token:
        abort(404)
    if request.method == "GET":
        if not db.pending_token_exists(token, "follow"):
            abort(404)
        return render_template("confirm.html", token=token, kind="alert",
                               action_url=url_for("follow_verify"))
    if db.verify_follow(token):
        return render_template("subscribe_ok.html", action="follow_verified")
    abort(404)


@app.route("/unfollow")
def unfollow():
    token = request.args.get("token", "")
    if token and db.unfollow(token):
        return render_template("subscribe_ok.html", action="unfollowed")
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


@app.route("/search")
def search_page():
    """Server-rendered search, so a query has a crawlable, shareable URL.

    The JS front end stays as-is for interactive filtering; this exists so a
    result set can be linked to and indexed, which a fragment (#q=) cannot be.
    """
    q = (request.args.get("q") or "").strip()[:200]
    page = max(1, request.args.get("page", 1, type=int))
    if not q:
        return redirect("/", code=302)

    result = db.search_reviews(q=q, sort="date", sort_dir="desc", page=page, per_page=50)
    slugs = db.slugs_for_reviews([r.get("book_key") for r in result["reviews"]])

    reviews = []
    for r in result["reviews"]:
        reviews.append({
            "title": r.get("book_title") or "",
            "author": " ".join(x for x in ((r.get("book_author_first_name") or "").strip(),
                                           (r.get("book_author_last_name") or "").strip()) if x),
            "reviewer": " ".join(x for x in ((r.get("reviewer_first_name") or "").strip(),
                                             (r.get("reviewer_last_name") or "").strip()) if x),
            "journal": r.get("publication_source") or "",
            "date": (r.get("publication_date") or "")[:10],
            "slug": slugs.get(r.get("book_key") or ""),
        })

    total = result["total"]
    total_pages = max(1, (total + 49) // 50)
    heading = f'Reviews matching \u201c{q}\u201d'
    return render_template(
        "search.html", q=q, heading=heading, reviews=reviews, total=total,
        page=min(page, total_pages), total_pages=total_pages,
        corpus=db.get_total_reviews(), sources=db.count_sources(),
        BASE_URL=BASE_URL)


@app.route("/book/<slug>")
def book_page(slug):
    """Every review of one book.

    This is the unit people actually search for ("reviews of X"), and unlike a
    per-review page it has real content: we hold no review text, so 227k
    one-line review pages would read as thin/doorway content (audit 2026-09-21).
    """
    data = db.get_book(slug)
    if not data:
        abort(404)
    book, rows = data["book"], data["reviews"]

    reviews, seen_subfields = [], []
    for r in rows:
        reviewer = " ".join(x for x in ((r.get("reviewer_first_name") or "").strip(),
                                        (r.get("reviewer_last_name") or "").strip()) if x)
        reviews.append({
            "reviewer": reviewer,
            "journal": r.get("publication_source") or "",
            "date": (r.get("publication_date") or "")[:10],
            "access": r.get("access_type") or "",
            "link": r.get("review_link") or "",
        })
        for code in (r.get("subfield_primary"), r.get("subfield_secondary")):
            if code and code in SUBFIELD_NAMES and code not in seen_subfields:
                seen_subfields.append(code)

    byline = " ".join(x for x in ((book.get("author_first") or "").strip(),
                                  (book.get("author_last") or "").strip()) if x)
    n = book["review_count"]
    review_count_text = f"{n:,} review{'' if n == 1 else 's'}"
    lo, hi = (book.get("first_year") or ""), (book.get("last_year") or "")
    years = lo if lo and lo == hi else (f"{lo}–{hi}" if lo and hi else "")

    also_by = []
    if book.get("author_last"):
        also_by = db.get_books_by_author(book["author_last"], exclude_slug=slug, limit=8)

    jsonld = {
        "@context": "https://schema.org",
        "@type": "Book",
        "name": book["title"],
        "url": f"{BASE_URL}/book/{book['slug']}",
    }
    if byline:
        jsonld["author"] = {"@type": "Person", "name": byline}
    graph = []
    for r, raw in zip(reviews, rows):
        node = {
            "@type": "Review",
            "itemReviewed": {"@type": "Book", "name": book["title"]},
            "publisher": {"@type": "Periodical", "name": r["journal"]},
        }
        if r["reviewer"]:
            node["author"] = {"@type": "Person", "name": r["reviewer"]}
        if r["date"]:
            node["datePublished"] = r["date"]
        if r["link"]:
            node["url"] = r["link"]
        graph.append(node)
    if graph:
        jsonld["review"] = graph

    return render_template(
        "book.html", book=book, reviews=reviews, byline=byline,
        review_count_text=review_count_text, years=years,
        subfields=[(c, SUBFIELD_NAMES[c]) for c in seen_subfields],
        also_by=also_by, jsonld=json.dumps(jsonld, ensure_ascii=False),
        BASE_URL=BASE_URL)


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


# One sitemap file holds 50,000 URLs; there are ~155k book pages, so
# /sitemap.xml is now an index and the URLs live in the files it names.
SITEMAP_PAGE_SIZE = 45000


@app.route("/sitemap.xml")
def sitemap_index():
    n_books = db.count_books()
    pages = (n_books + SITEMAP_PAGE_SIZE - 1) // SITEMAP_PAGE_SIZE
    xml = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
           f'<sitemap><loc>{BASE_URL}/sitemap-pages.xml</loc></sitemap>']
    for i in range(1, pages + 1):
        xml.append(f'<sitemap><loc>{BASE_URL}/sitemap-books-{i}.xml</loc></sitemap>')
    xml.append('</sitemapindex>')
    resp = make_response("\n".join(xml))
    resp.headers["Content-Type"] = "application/xml"
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


@app.route("/sitemap-books-<int:part>.xml")
def sitemap_books(part):
    """Book pages, most-reviewed first so crawl budget lands on the best ones."""
    if part < 1:
        abort(404)
    rows = db.get_books_for_sitemap(
        limit=SITEMAP_PAGE_SIZE, offset=(part - 1) * SITEMAP_PAGE_SIZE)
    if not rows:
        abort(404)
    xml = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for r in rows:
        # a book reviewed many times is a better landing page than a singleton
        priority = "0.7" if r["review_count"] >= 3 else ("0.6" if r["review_count"] >= 2 else "0.4")
        xml.append(f'<url><loc>{BASE_URL}/book/{escape(r["slug"])}</loc>'
                   f'<changefreq>monthly</changefreq><priority>{priority}</priority></url>')
    xml.append('</urlset>')
    resp = make_response("\n".join(xml))
    resp.headers["Content-Type"] = "application/xml"
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


@app.route("/sitemap-pages.xml")
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

# Generous enough that no human — or our own front end, which fires a request
# per filter change — will ever reach it, but it turns "pull the whole database
# in ~910 requests" into a crawl. The limiter is per-process and we run a single
# gunicorn worker; this becomes per-worker if that ever changes.
_API_RATE_LIMIT = 120
_API_RATE_WINDOW = 60


@app.route("/api/reviews")
def api_reviews():
    if not _rate_limiter.allow(f"api:{_client_ip()}", _API_RATE_LIMIT, _API_RATE_WINDOW):
        resp = jsonify({"error": "Too many requests. Please slow down."})
        resp.status_code = 429
        resp.headers["Retry-After"] = str(_API_RATE_WINDOW)
        return resp

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


# Content-Security-Policy, assembled from what the pages actually load:
#   scripts  — our own /static/app.js, GoatCounter's counter, inline blocks
#   styles   — our own stylesheet, Google Fonts, inline style="" attributes
#   fonts    — Google's font CDN
#   connect  — our own /api/*, plus GoatCounter's hit endpoint
# Shipped report-only first: a policy that silently blanks the page is worse
# than no policy, so we collect violations at /csp-report before enforcing.
_CSP = "; ".join([
    "default-src 'self'",
    "script-src 'self' 'unsafe-inline' https://gc.zgo.at",
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
    "font-src 'self' https://fonts.gstatic.com",
    "img-src 'self' data:",
    "connect-src 'self' https://philreviews.goatcounter.com",
    "form-action 'self'",
    "frame-ancestors 'none'",
    "base-uri 'self'",
    "object-src 'none'",
    "report-uri /csp-report",
])

_SECURITY_HEADERS = {
    # 1 year; no preload — that list is painful to leave
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Permissions-Policy": "geolocation=(), camera=(), microphone=(), payment=(), usb=()",
    "Cross-Origin-Opener-Policy": "same-origin",
}


# Violations are tallied on the persistent volume, not just the app log. Fly's
# logs rotate, and the weekly sync replaces reviews.db wholesale — so neither
# survives the week we need to watch before enforcing the policy.
CSP_DB_PATH = os.environ.get(
    "CSP_DB_PATH", os.path.join(os.path.dirname(db.SUBSCRIBERS_DB_PATH), "csp_reports.db"))


def _csp_store(blocked, directive, document):
    """Record one violation, keeping a count per distinct (directive, blocked)."""
    import sqlite3
    conn = sqlite3.connect(CSP_DB_PATH, timeout=5)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS csp_violations (
            directive TEXT NOT NULL, blocked TEXT NOT NULL,
            hits INTEGER NOT NULL DEFAULT 0,
            first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
            sample_document TEXT,
            PRIMARY KEY (directive, blocked))""")
        now = datetime.utcnow().isoformat(timespec="seconds")
        conn.execute("""INSERT INTO csp_violations
              (directive, blocked, hits, first_seen, last_seen, sample_document)
              VALUES (?,?,1,?,?,?)
            ON CONFLICT(directive, blocked) DO UPDATE SET
              hits = hits + 1, last_seen = excluded.last_seen""",
            (directive, blocked, now, now, document))
        conn.commit()
    finally:
        conn.close()


@app.route("/csp-report", methods=["POST"])
def csp_report():
    """Collect CSP violations while the policy is report-only.

    Rate limited per IP: a report endpoint is an open write path, and browsers
    send one report per blocked resource per page view.
    """
    if not _rate_limiter.allow(f"csp:{_client_ip()}", 20, 3600):
        return "", 204
    try:
        payload = request.get_json(force=True, silent=True) or {}
        report = payload.get("csp-report", payload)
        blocked = str(report.get("blocked-uri") or "")[:300]
        directive = str(report.get("violated-directive") or "")[:100]
        document = str(report.get("document-uri") or "")[:300]
        app.logger.warning("CSP violation: blocked=%s directive=%s on %s",
                           blocked, directive, document)
        _csp_store(blocked, directive, document)
    except Exception:
        app.logger.exception("could not record CSP violation")
    return "", 204


@app.route("/csp-status")
def csp_status():
    """Summary of collected violations, so the policy can be judged remotely."""
    import sqlite3
    enforcing = "Content-Security-Policy" in _SECURITY_HEADERS
    try:
        conn = sqlite3.connect(CSP_DB_PATH, timeout=5)
        conn.row_factory = sqlite3.Row
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM csp_violations ORDER BY hits DESC LIMIT 50")]
        conn.close()
    except Exception:
        rows = []
    return jsonify({
        "mode": "enforcing" if enforcing else "report-only",
        "distinct_violations": len(rows),
        "total_hits": sum(r["hits"] for r in rows),
        "violations": rows,
        "verdict": ("No violations recorded — safe to enforce." if not rows
                    else "Review the entries below before enforcing."),
    })


@app.after_request
def add_cache_headers(response):
    if request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=86400"
    for header, value in _SECURITY_HEADERS.items():
        response.headers.setdefault(header, value)
    # HTML only: a CSP on a JSON body or a font buys nothing
    if response.mimetype == "text/html":
        response.headers.setdefault("Content-Security-Policy-Report-Only", _CSP)
    return response


if __name__ == "__main__":
    app.run(debug=os.environ.get("FLASK_DEBUG") == "1")
