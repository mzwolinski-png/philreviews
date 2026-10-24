"""Model check: is this book of philosophical interest?

For general-interest outlets whose review sections range well beyond
philosophy (Law & Liberty: presidential biographies, novels, trade policy).
The tier-1 filter's matching can't tell these apart -- strict title matching
on Law & Liberty would have cut Dostoevsky Reads Hegel and kept '13 Novels
Conservatives Will Love' (dry run, 2026-10-24).

Leans hard toward keeping: Matt prefers over-inclusion, and anything the
model can't judge, or any API failure, is kept.
"""
import json
import logging
import re

log = logging.getLogger(__name__)

MODEL = "claude-sonnet-5-5"

_PROMPT = """You screen book reviews for PhilReviews, an index of reviews of books of interest to academic philosophers.

KEEP a book if a philosopher could plausibly want to know it was reviewed: philosophy proper; political, legal, moral or social theory; intellectual history and the history of ideas; political economy and its foundations; religion and theology at the level of ideas; studies of a thinker; serious books about education, science, technology or culture as ideas.

DROP only books that are plainly something else: narrative or military history, biography of a politician or general, fiction, poetry, memoir, current-affairs or policy books about a specific issue, business, journalism.

When unsure, KEEP.

Reply with ONLY a JSON array, one object per item, in order:
[{"item": 1, "keep": true, "reason": "<= 8 words"}, ...]

Items:
"""


def screen(records, client=None):
    """Return (kept, dropped) lists. Each dropped record gets '_gate_reason'.

    records: dicts with 'Book Title', 'Book Author First Name',
    'Book Author Last Name' and optionally '_excerpt'.
    """
    if not records:
        return [], []
    try:
        if client is None:
            from anthropic import Anthropic
            client = Anthropic()
    except Exception as e:
        log.warning(f"Relevance gate unavailable, keeping all: {e}")
        return list(records), []

    kept, dropped = [], []
    for i in range(0, len(records), 10):
        batch = records[i:i + 10]
        items = ""
        for j, rec in enumerate(batch, 1):
            author = f"{rec.get('Book Author First Name', '')} {rec.get('Book Author Last Name', '')}".strip()
            items += f"\n{j}. {rec.get('Book Title', '')}" + (f" -- by {author}" if author else "")
            if rec.get("_excerpt"):
                items += f"\n   Review blurb: {rec['_excerpt'][:400]}"
        verdicts = {}
        try:
            resp = client.messages.create(
                model=MODEL, max_tokens=4000,
                messages=[{"role": "user", "content": _PROMPT + items}],
            )
            text = "".join(b.text for b in resp.content if getattr(b, "type", "") == "text")
            m = re.search(r"\[.*\]", text, re.S)
            for v in json.loads(m.group()) if m else []:
                verdicts[int(v.get("item", 0))] = v
        except Exception as e:
            log.warning(f"Relevance gate batch failed, keeping it: {e}")
        for j, rec in enumerate(batch, 1):
            v = verdicts.get(j)
            if v is not None and v.get("keep") is False:
                rec["_gate_reason"] = str(v.get("reason", ""))[:80]
                dropped.append(rec)
            else:
                kept.append(rec)
    return kept, dropped
