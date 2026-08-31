#!/usr/bin/env python3
"""One-off survey: which PDCNET journals have detectable book reviews in Crossref?
Kill-safe / resumable: appends one JSON line per journal to pdcnet_survey_results.jsonl.
Re-running skips journals already recorded. Run the reporter block at the end
(or pdcnet_report.py) to print the ranked table."""
import json, urllib.request, re, time, os

UA = {'User-Agent': 'PhilReviews/2.0 (mailto:mzwolinski@sandiego.edu)'}
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(ROOT, 'scripts', 'pdcnet_survey_results.jsonl')


def cj(url, tries=3):
    for i in range(tries):
        try:
            return json.load(urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=40))
        except urllib.error.HTTPError as e:
            if e.code in (429, 403):
                time.sleep(20)
            elif i == tries - 1:
                return {'_err': f'HTTP {e.code}'}
        except Exception as e:
            if i == tries - 1:
                return {'_err': str(e)[:50]}
            time.sleep(4)
    return {'_err': 'retries'}


def pages(it):
    m = re.match(r'(\d+)\D+(\d+)', it.get('page', '') or '')
    return (int(m.group(2)) - int(m.group(1)) + 1) if m else None


def is_review(t):
    t = t or ''
    return bool(re.match(r'^\s*review\b', t, re.I)) or bool(re.search(r',\s+by\s+[A-Z]', t))


covered = set()
try:
    covered = set(re.findall(r"^\s{4}'([^']+)':\s*\{", open(os.path.join(ROOT, 'journals.py')).read(), re.M))
except Exception:
    pass

done_journals = set()
if os.path.exists(RESULTS):
    for line in open(RESULTS):
        try:
            done_journals.add(json.loads(line)['journal'])
        except Exception:
            pass
print(f"resuming: {len(done_journals)} journals already recorded", flush=True)

mem = cj('https://api.crossref.org/members?query=Philosophy+Documentation+Center&rows=5')
mid = next((m['id'] for m in mem.get('message', {}).get('items', [])
            if 'philosophy documentation' in m.get('primary-name', '').lower()), None)
if not mid:
    print("no member id", flush=True); raise SystemExit
fac = cj(f'https://api.crossref.org/works?filter=member:{mid}&facet=issn:300&rows=0')
issns = fac.get('message', {}).get('facets', {}).get('issn', {}).get('values', {})
cand = sorted(([i.rsplit('/', 1)[-1], c] for i, c in issns.items() if c >= 60), key=lambda x: -x[1])
print(f"member {mid}: {len(cand)} ISSNs (>=60 works) to consider", flush=True)

out = open(RESULTS, 'a')
seen_titles = set(done_journals)
n_new = 0
for k, (issn, total) in enumerate(cand):
    d = cj(f'https://api.crossref.org/journals/{issn}/works?rows=100&select=title,page,container-title')
    time.sleep(1.6)
    items = d.get('message', {}).get('items', []) if '_err' not in d else []
    if not items:
        continue
    jtitle = (items[0].get('container-title') or ['(unknown)'])[0]
    if jtitle in seen_titles:
        continue
    seen_titles.add(jtitle)
    n = len(items)
    sig = sum(is_review((it.get('title') or [''])[0]) for it in items)
    rec = {'journal': jtitle, 'issn': issn, 'total': total, 'sampled': n,
           'sig': sig, 'pct': round(sig / n, 3) if n else 0,
           'est_reviews': int(total * (sig / n)) if n else 0,
           'covered': jtitle in covered}
    out.write(json.dumps(rec) + '\n'); out.flush()
    n_new += 1
    if n_new % 5 == 0:
        print(f"  ...{n_new} new journals recorded ({k+1}/{len(cand)} ISSNs)", flush=True)
out.close()
print(f"DONE: {n_new} new journals recorded this run", flush=True)
