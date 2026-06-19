#!/usr/bin/env python3
"""Weekly review app — interactive Keep / Flag / Reject for new entries.

Persistent local Flask app showing every entry still awaiting review
(``reviewed = 0``), with auto-flagged suspects pre-highlighted. The headless
weekly ``update.py`` run leaves new entries at ``reviewed = 0``; this app lets
the user triage them whenever:

  Keep   -> mark reviewed (stays live)
  Flag   -> stays live, your note saved to review_flags for Claude to act on
  Reject -> deleted (+ DOI excluded so it won't re-import)

On submit, decisions are applied to the local DB and synced to production in
the background. Run:  python3 review_app.py  (then open http://localhost:8770)
"""

import html
import sqlite3
import threading

from flask import Flask, request, jsonify

import db
try:
    from scripts.integrity_check import _author_junk_reasons
except Exception:
    def _author_junk_reasons(s):
        return []

PORT = 8770
app = Flask(__name__)


def _suspect_reasons(af, al, title, entry_type):
    reasons = [f"author:{w}" for w in _author_junk_reasons(af)]
    reasons += [f"author:{w}" for w in _author_junk_reasons(al)]
    if not af and not al and (entry_type or "review") != "symposium":
        reasons.append("missing-author")
    if "<" in (title or ""):
        reasons.append("title:html")
    if title and " " not in title and any(
            title[i].islower() and title[i + 1].isupper() for i in range(len(title) - 1)):
        reasons.append("title:concatenated")
    return sorted(set(reasons))


def load_pending():
    conn = db._connect()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, book_title, book_author_first_name, book_author_last_name, "
        "reviewer_first_name, reviewer_last_name, publication_source, publication_date, "
        "subfield_primary, review_link, entry_type, doi "
        "FROM reviews WHERE reviewed = 0 ORDER BY publication_source, id"
    ).fetchall()
    conn.close()
    items = []
    for r in rows:
        af, al = r["book_author_first_name"] or "", r["book_author_last_name"] or ""
        reasons = _suspect_reasons(af, al, r["book_title"], r["entry_type"])
        items.append({
            "id": r["id"],
            "title": r["book_title"] or "(no title)",
            "author": f"{af} {al}".strip() or "—",
            "reviewer": f"{r['reviewer_first_name'] or ''} {r['reviewer_last_name'] or ''}".strip() or "—",
            "source": r["publication_source"] or "—",
            "date": (r["publication_date"] or "")[:10],
            "subfield": r["subfield_primary"] or "—",
            "link": r["review_link"] or "",
            "suspect": bool(reasons),
            "reasons": reasons,
        })
    items.sort(key=lambda x: (not x["suspect"], x["source"].lower(), x["id"]))
    return items


def apply_decisions(accept, reject, flags):
    conn = db._connect()
    conn.row_factory = sqlite3.Row
    for rid in accept:
        conn.execute("UPDATE reviews SET reviewed = 1 WHERE id = ?", (rid,))
    for f in flags:
        conn.execute("UPDATE reviews SET reviewed = 1 WHERE id = ?", (f["id"],))
        conn.execute("INSERT INTO review_flags (review_id, note) VALUES (?, ?)",
                     (f["id"], (f.get("note") or "").strip()))
    for rid in reject:
        row = conn.execute("SELECT doi FROM reviews WHERE id = ?", (rid,)).fetchone()
        if row and row["doi"]:
            conn.execute("INSERT OR IGNORE INTO excluded_dois (doi, reason) VALUES (?, ?)",
                         (row["doi"], "Rejected in weekly review"))
        conn.execute("DELETE FROM reviews WHERE id = ?", (rid,))
    conn.commit()
    conn.close()


def background_sync():
    try:
        from deploy import sync_to_fly
        sync_to_fly()
    except Exception:
        pass


PAGE = """<!doctype html><html><head><meta charset="utf-8">
<title>Weekly review</title>
<style>
:root{--keep:#1a7f4b;--flag:#b8860b;--rej:#b3261e;--bg:#f6f7f9;--card:#fff;--line:#e3e6ea;--navy:#1a3a5c}
*{box-sizing:border-box}body{font:15px/1.45 -apple-system,Segoe UI,Roboto,sans-serif;margin:0;background:var(--bg);color:#1b1f24;padding-bottom:96px}
header{background:var(--navy);color:#fff;padding:18px 22px}header h1{margin:0;font-size:19px}header p{margin:6px 0 0;opacity:.85;font-size:13px}
.wrap{max-width:1000px;margin:18px auto;padding:0 16px}
.sec{font-size:13px;text-transform:uppercase;letter-spacing:.05em;color:#6b7480;margin:22px 4px 8px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px;margin:8px 0}
.card.suspect{border-left:4px solid var(--flag)}
.card.reject{opacity:.5;background:#fbeceb}.card.reject .title{text-decoration:line-through}
.card.flag{background:#fff9e8}
.row{display:flex;gap:14px;align-items:flex-start}
.meta{flex:1;min-width:0}.title{font-weight:600}.by{color:#444}.sub{color:#6b7480;font-size:13px;margin-top:3px}
.badge{display:inline-block;font-size:11px;padding:2px 7px;border-radius:20px;margin-right:6px;vertical-align:1px}
.b-src{background:#eef0f2;color:#444}.b-sf{background:#e4f0ff;color:#1b4f8a}.b-sus{background:#fff2d6;color:#8a5a00}
a.lk{color:var(--navy);font-size:13px}
.btns{display:flex;gap:6px;flex-shrink:0}
.tg{border:1px solid var(--line);background:#fff;border-radius:7px;padding:7px 11px;cursor:pointer;font-weight:600;font-size:13px}
.tg.k.on{background:var(--keep);color:#fff;border-color:var(--keep)}
.tg.f.on{background:var(--flag);color:#fff;border-color:var(--flag)}
.tg.r.on{background:var(--rej);color:#fff;border-color:var(--rej)}
.note{display:none;margin-top:10px;width:100%;border:1px solid var(--line);border-radius:7px;padding:8px;font:14px inherit;resize:vertical}
.card.flag .note{display:block}
footer{position:fixed;bottom:0;left:0;right:0;background:#fff;border-top:1px solid var(--line);padding:14px 22px;display:flex;align-items:center;gap:16px;box-shadow:0 -2px 10px rgba(0,0,0,.05)}
#tally{font-size:14px}.k{color:var(--keep);font-weight:600}.f{color:var(--flag);font-weight:600}.r{color:var(--rej);font-weight:600}
#go{margin-left:auto;background:var(--navy);color:#fff;border:0;border-radius:8px;padding:11px 22px;font-size:15px;font-weight:600;cursor:pointer}
#go:disabled{opacity:.5}
#done{display:none;text-align:center;padding:60px 20px}#done h2{color:var(--keep)}
.toolbar{margin:4px;font-size:13px}.toolbar button{background:#fff;border:1px solid var(--line);border-radius:6px;padding:5px 10px;cursor:pointer;margin-right:6px}
.empty{text-align:center;padding:80px 20px;color:#6b7480}
</style></head><body>
<header><h1>Weekly review</h1>
<p>{{N}} entries awaiting review. Default is <b>Keep</b>. <b>Flag</b> keeps it live and saves a note for Claude; <b>Reject</b> deletes it. Suspect entries (auto-detected) are highlighted and listed first.</p></header>
<div id="app" class="wrap">{{BODY}}</div>
<div id="done"><h2>✓ Submitted</h2><p id="dmsg"></p><p>Decisions applied and syncing to production. You can close this tab.</p></div>
<footer id="ft"><span id="tally"></span><button id="go" onclick="submit()">Submit decisions</button></footer>
<script>
const dec={},notes={};
function set(id,v){dec[id]=v;render(id);tally();}
function note(id,el){notes[id]=el.value;}
function all(v){document.querySelectorAll('[data-id]').forEach(c=>{dec[c.dataset.id]=v;render(c.dataset.id);});tally();}
function render(id){const c=document.querySelector('[data-id="'+id+'"]');const v=dec[id];
 c.classList.toggle('reject',v==='reject');c.classList.toggle('flag',v==='flag');
 c.querySelector('.k').classList.toggle('on',v==='keep');
 c.querySelector('.f').classList.toggle('on',v==='flag');
 c.querySelector('.r').classList.toggle('on',v==='reject');}
function tally(){const ids=Object.keys(dec);const r=ids.filter(i=>dec[i]==='reject').length,f=ids.filter(i=>dec[i]==='flag').length;
 document.getElementById('tally').innerHTML='<span class="k">'+(ids.length-r-f)+' keep</span> · <span class="f">'+f+' flag</span> · <span class="r">'+r+' reject</span>';}
async function submit(){document.getElementById('go').disabled=true;
 const accept=[],reject=[],flags=[];
 Object.keys(dec).forEach(i=>{const id=Number(i);
  if(dec[i]==='reject')reject.push(id);
  else if(dec[i]==='flag')flags.push({id:id,note:notes[i]||''});
  else accept.push(id);});
 const res=await fetch('/submit',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({accept,reject,flags})});
 const j=await res.json();
 document.getElementById('app').style.display='none';document.getElementById('ft').style.display='none';
 document.getElementById('dmsg').textContent=j.message;document.getElementById('done').style.display='block';}
document.querySelectorAll('[data-id]').forEach(c=>dec[c.dataset.id]='keep');tally();
</script></body></html>"""


def render_body(items):
    if not items:
        return '<div class="empty"><h2>✓ All caught up</h2><p>Nothing awaiting review.</p></div>'
    out = ['<div class="toolbar"><button onclick="all(\'keep\')">Keep all</button>'
           '<button onclick="all(\'reject\')">Reject all</button></div>']
    last = None
    for it in items:
        sec = "⚠ Suspect — auto-flagged" if it["suspect"] else f"{it['source']}"
        cur = "SUSPECT" if it["suspect"] else it["source"]
        if cur != last:
            out.append(f'<div class="sec">{html.escape(sec)}</div>')
            last = cur
        sus = (f'<span class="badge b-sus" title="{html.escape(", ".join(it["reasons"]))}">SUSPECT</span>'
               if it["suspect"] else "")
        link = (f'<a class="lk" href="{html.escape(it["link"])}" target="_blank" rel="noopener">open ↗</a>'
                if it["link"] else "")
        out.append(f'''<div class="card{' suspect' if it['suspect'] else ''}" data-id="{it['id']}">
<div class="row"><div class="meta">
<div class="title">{html.escape(it['title'])}</div>
<div class="by">by {html.escape(it['author'])} · reviewed by {html.escape(it['reviewer'])} · {html.escape(it['date'])}</div>
<div class="sub">{sus}<span class="badge b-src">{html.escape(it['source'])}</span><span class="badge b-sf">{html.escape(it['subfield'])}</span> {link}</div>
</div>
<div class="btns"><button class="tg k on" onclick="set({it['id']},'keep')">Keep</button>
<button class="tg f" onclick="set({it['id']},'flag')">Flag</button>
<button class="tg r" onclick="set({it['id']},'reject')">Reject</button></div></div>
<textarea class="note" placeholder="Note for Claude (what's wrong / what to do)…" oninput="note({it['id']},this)"></textarea>
</div>''')
    return "\n".join(out)


@app.route("/")
def index():
    items = load_pending()
    return PAGE.replace("{{N}}", str(len(items))).replace("{{BODY}}", render_body(items))


@app.route("/submit", methods=["POST"])
def submit():
    data = request.get_json(force=True)
    accept = [int(i) for i in data.get("accept", [])]
    reject = [int(i) for i in data.get("reject", [])]
    flags = [{"id": int(f["id"]), "note": f.get("note", "")} for f in data.get("flags", [])]
    apply_decisions(accept, reject, flags)
    threading.Thread(target=background_sync, daemon=True).start()
    return jsonify({"message": f"{len(accept)} kept, {len(flags)} flagged, {len(reject)} rejected."})


if __name__ == "__main__":
    app.run(port=PORT, debug=False)
