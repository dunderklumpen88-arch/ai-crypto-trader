import unittest
from trader_intelligence import trader_intelligence_adjustment, collect_public_signals

class TraderIntelligenceTests(unittest.TestCase):
    def test_unverified_signal_has_no_effect(self):
        out = trader_intelligence_adjustment(2, {"product": "BTC-EUR"}, [
            {"product": "BTC-EUR", "direction": "LONG", "quality": 0.99,
             "sample_count": 100, "max_drawdown_pct": 2,
             "verified_track_record": False,
             "observed_at": "2099-01-01T00:00:00+00:00"}
        ])
        self.assertEqual(out["adjustment"], 0.0)
        self.assertEqual(out["sources_used"], 0)

    def test_disabled_by_default(self):
        out = collect_public_signals()
        self.assertFalse(out["enabled"])
        self.assertEqual(out["signals"], [])

if __name__ == "__main__":
    unittest.main()
