"""Unit tests for app.py abuse-protection + EBSCO-link helpers."""
import os, sys, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import app


class ValidEmail(unittest.TestCase):
    def test_accepts_normal(self):
        self.assertTrue(app._valid_email("a@b.com"))
        self.assertTrue(app._valid_email("first.last@uni.edu"))

    def test_rejects_malformed(self):
        for bad in ("", "noat", "a@b", "a@@b.com", "a b@c.com"):
            self.assertFalse(app._valid_email(bad), bad)

    def test_rejects_crlf_header_injection(self):
        self.assertFalse(app._valid_email("victim@x.com\nBcc: evil@y.com"))
        self.assertFalse(app._valid_email("a@b.com\r\nSubject: spam"))

    def test_rejects_overlong(self):
        self.assertFalse(app._valid_email("a@" + "b" * 300 + ".com"))


class RateLimiter(unittest.TestCase):
    def test_allows_up_to_limit_then_blocks(self):
        rl = app._RateLimiter()
        allowed = sum(rl.allow("k", 3, 3600) for _ in range(5))
        self.assertEqual(allowed, 3)

    def test_keys_independent(self):
        rl = app._RateLimiter()
        self.assertTrue(rl.allow("a", 1, 3600))
        self.assertTrue(rl.allow("b", 1, 3600))
        self.assertFalse(rl.allow("a", 1, 3600))


class SearchLink(unittest.TestCase):
    def test_builds_google_search_no_ebsco(self):
        url = app._search_link("Some Book", "Jane Reviewer", "Ann Author", "Mind")
        self.assertTrue(url.startswith("https://www.google.com/search?q="))
        self.assertNotIn("ebsco", url.lower())
        self.assertIn("Some+Book", url)


if __name__ == "__main__":
    unittest.main()
