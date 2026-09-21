"""Security headers, CSP report-only, and API rate limiting."""
import os, sys, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import app as appmod


class SecurityHeaders(unittest.TestCase):
    def setUp(self):
        appmod.app.config["TESTING"] = True
        self.c = appmod.app.test_client()
        appmod._rate_limiter._hits.clear()

    def test_headers_present_on_html(self):
        r = self.c.get("/")
        for h in ("Strict-Transport-Security", "X-Content-Type-Options",
                  "X-Frame-Options", "Referrer-Policy", "Permissions-Policy"):
            self.assertIn(h, r.headers, h)
        self.assertEqual(r.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(r.headers["X-Frame-Options"], "DENY")

    def test_csp_is_report_only_on_html(self):
        r = self.c.get("/")
        self.assertIn("Content-Security-Policy-Report-Only", r.headers)
        # enforcing header must NOT be set yet
        self.assertNotIn("Content-Security-Policy", r.headers)

    def test_csp_allows_what_the_pages_actually_load(self):
        csp = self.c.get("/").headers["Content-Security-Policy-Report-Only"]
        for needed in ("https://gc.zgo.at", "https://fonts.googleapis.com",
                       "https://fonts.gstatic.com", "https://philreviews.goatcounter.com"):
            self.assertIn(needed, csp, needed)
        self.assertIn("frame-ancestors 'none'", csp)

    def test_no_csp_on_json(self):
        r = self.c.get("/api/reviews?per_page=1")
        self.assertNotIn("Content-Security-Policy-Report-Only", r.headers)
        self.assertIn("X-Content-Type-Options", r.headers)   # plain headers still apply


class ApiRateLimit(unittest.TestCase):
    def setUp(self):
        appmod.app.config["TESTING"] = True
        self.c = appmod.app.test_client()
        appmod._rate_limiter._hits.clear()

    def tearDown(self):
        appmod._rate_limiter._hits.clear()

    def test_normal_use_is_never_limited(self):
        for _ in range(30):
            self.assertEqual(self.c.get("/api/reviews?per_page=1").status_code, 200)

    def test_limit_eventually_trips_with_retry_after(self):
        last = None
        for _ in range(appmod._API_RATE_LIMIT + 5):
            last = self.c.get("/api/reviews?per_page=1")
        self.assertEqual(last.status_code, 429)
        self.assertIn("Retry-After", last.headers)

    def test_other_pages_are_not_limited_by_api_usage(self):
        for _ in range(appmod._API_RATE_LIMIT + 5):
            self.c.get("/api/reviews?per_page=1")
        self.assertEqual(self.c.get("/").status_code, 200)


class CspReportEndpoint(unittest.TestCase):
    def setUp(self):
        appmod.app.config["TESTING"] = True
        self.c = appmod.app.test_client()
        appmod._rate_limiter._hits.clear()

    def test_accepts_a_report(self):
        r = self.c.post("/csp-report", json={"csp-report": {
            "blocked-uri": "https://evil.example/x.js",
            "violated-directive": "script-src",
            "document-uri": "https://philreviews.org/"}})
        self.assertEqual(r.status_code, 204)

    def test_survives_garbage(self):
        self.assertEqual(self.c.post("/csp-report", data="not json").status_code, 204)
