"""Law & Liberty relevance gate (2026-10-24): drops plainly non-philosophy
books, and keeps everything when it can't judge."""
import os
import sys
import unittest
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import relevance_gate  # noqa: E402


def _client(reply=None, error=None):
    def create(**kw):
        if error:
            raise error
        # Sonnet may lead with a thinking block; only text blocks carry the answer
        return NS(content=[NS(type="thinking", thinking="..."), NS(type="text", text=reply)])
    return NS(messages=NS(create=create))


RECS = [{"Book Title": "Dostoevsky Reads Hegel", "Book Author Last Name": "Foldenyi"},
        {"Book Title": "The Last Campaign", "Book Author Last Name": "Brands"}]


class Gate(unittest.TestCase):
    def test_drops_only_what_the_model_rejects(self):
        reply = '[{"item": 1, "keep": true}, {"item": 2, "keep": false, "reason": "military history"}]'
        kept, dropped = relevance_gate.screen([dict(r) for r in RECS], _client(reply))
        self.assertEqual([r["Book Title"] for r in kept], ["Dostoevsky Reads Hegel"])
        self.assertEqual(dropped[0]["_gate_reason"], "military history")

    def test_api_failure_keeps_everything(self):
        kept, dropped = relevance_gate.screen([dict(r) for r in RECS], _client(error=RuntimeError("down")))
        self.assertEqual((len(kept), len(dropped)), (2, 0))

    def test_unparseable_or_missing_verdicts_keep(self):
        kept, dropped = relevance_gate.screen([dict(r) for r in RECS], _client('[{"item": 1, "keep": false}]x'))
        # item 1 rejected, item 2 has no verdict -> kept
        self.assertEqual([r["Book Title"] for r in kept], ["The Last Campaign"])
        kept, dropped = relevance_gate.screen([dict(r) for r in RECS], _client("no json here"))
        self.assertEqual(len(kept), 2)


if __name__ == "__main__":
    unittest.main()
