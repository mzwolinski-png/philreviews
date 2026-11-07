import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from crossref_scraper import apply_reviews_floor


def item(*parts):
    return {'issued': {'date-parts': [list(parts)]}}


class ReviewsFloor(unittest.TestCase):
    def test_old_reindexed_records_dropped(self):
        kept = apply_reviews_floor([item(1982, 4), item(2026, 11, 2), item(2014)], '2014-01-01')
        self.assertEqual(kept, [item(2026, 11, 2), item(2014)])

    def test_no_floor_keeps_all(self):
        items = [item(1956), item(2026)]
        self.assertEqual(apply_reviews_floor(items, None), items)


if __name__ == '__main__':
    unittest.main()
