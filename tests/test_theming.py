"""Design tokens, dark mode, and the theme toggle.

The stylesheet had 59 hard-coded colours and no custom properties, so a
re-theme meant find-and-replace across 1,030 lines and dark mode was
impractical (audit 2026-09-21). These tests pin the structure that fixed it.
"""
import os
import re
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import app as appmod

CSS = open(os.path.join(ROOT, "static", "style.css")).read()


def _tokens(block_src):
    return {k: v.strip() for k, v in re.findall(r"--([\w-]+):\s*([^;]+);", block_src)}


def _root():
    return _tokens(CSS.split(":root {", 1)[1].split("\n}", 1)[0])


def _dark():
    return _tokens(re.search(r':root\[data-theme="dark"\] \{(.*?)\n\}', CSS, re.S).group(1))


def _contrast(a, b):
    def rgb(h):
        h = h.lstrip("#")
        if len(h) == 3:
            h = "".join(c * 2 for c in h)
        return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))

    def lum(h):
        def f(c):
            c /= 255
            return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
        r, g, bl = rgb(h)
        return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(bl)
    la, lb = lum(a), lum(b)
    hi, lo = max(la, lb), min(la, lb)
    return (hi + 0.05) / (lo + 0.05)


class Tokens(unittest.TestCase):
    def test_no_bare_hex_outside_the_token_blocks(self):
        """Component rules must go through tokens, or a re-theme misses them."""
        offenders = []
        for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", CSS):
            selector, decls = m.group(1).strip(), m.group(2)
            if ":root" in selector:          # the token definitions themselves
                continue
            for hexv in re.findall(r"#[0-9a-fA-F]{3,8}\b", decls):
                offenders.append((selector.splitlines()[-1].strip()[:40], hexv))
        self.assertEqual(offenders, [], f"un-tokenised colours: {offenders[:8]}")

    def test_every_var_reference_resolves(self):
        used = set(re.findall(r"var\(--([\w-]+)\)", CSS))
        self.assertEqual(used - set(_root()), set())

    def test_dark_overrides_every_token(self):
        self.assertEqual(set(_root()) - set(_dark()), set(),
                         "a token not overridden in dark renders light-on-dark")


class DarkMode(unittest.TestCase):
    def test_media_query_yields_to_an_explicit_light_choice(self):
        # without the :not() guard, choosing light on a dark OS does nothing
        self.assertIn('prefers-color-scheme: dark', CSS)
        self.assertIn(':root:not([data-theme="light"])', CSS)

    def test_explicit_dark_attribute_is_styled(self):
        self.assertIn(':root[data-theme="dark"]', CSS)

    def test_color_scheme_is_declared(self):
        # makes form controls and scrollbars follow the theme
        self.assertIn("color-scheme", CSS)

    def test_body_paints_its_own_background(self):
        body = re.search(r"\nbody \{(.*?)\n\}", CSS, re.S).group(1)
        self.assertIn("background", body)


class Contrast(unittest.TestCase):
    PAIRS = [("ink", "bg"), ("ink", "surface"), ("ink-3", "surface"),
             ("ink-4", "surface"), ("ink-7", "surface"), ("brand", "surface"),
             ("accent", "surface"), ("ok-fg", "ok-bg"), ("err-fg", "err-bg"),
             ("warn-fg", "warn-bg"), ("note-fg", "note-bg")]

    def test_light_theme_meets_aa(self):
        t = _root()
        for fg, bg in self.PAIRS:
            with self.subTest(pair=f"{fg}/{bg}"):
                self.assertGreaterEqual(round(_contrast(t[fg], t[bg]), 2), 4.5)

    def test_dark_theme_meets_aa(self):
        light, dark = _root(), _dark()
        t = {**light, **dark}
        for fg, bg in self.PAIRS:
            with self.subTest(pair=f"{fg}/{bg}"):
                self.assertGreaterEqual(round(_contrast(t[fg], t[bg]), 2), 4.5)


class Toggle(unittest.TestCase):
    def setUp(self):
        appmod.app.config["TESTING"] = True
        self.c = appmod.app.test_client()
        appmod._rate_limiter._hits.clear()

    def test_every_page_carries_the_toggle_and_the_no_flash_script(self):
        for url in ("/", "/journals", "/subfields", "/changelog",
                    "/journal/Mind", "/subfield/ethics", "/search?q=kant"):
            with self.subTest(url=url):
                html = self.c.get(url).get_data(as_text=True)
                self.assertIn('id="theme-toggle"', html)
                self.assertIn("pr-theme", html)

    def test_no_flash_script_runs_before_the_stylesheet(self):
        html = self.c.get("/").get_data(as_text=True)
        self.assertLess(html.index("pr-theme"), html.index("style.css"),
                        "theme must be applied before first paint")

    def test_toggle_is_labelled_for_screen_readers(self):
        html = self.c.get("/").get_data(as_text=True)
        self.assertIn('aria-label="Switch between light and dark theme"', html)
