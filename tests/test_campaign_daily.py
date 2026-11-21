"""Tests for pipeline/campaign_daily.py aggregation."""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline"))

import campaign_daily  # noqa: E402
import clicks  # noqa: E402
import events  # noqa: E402

DAY = 20_000  # 2024-10-04, arbitrary
NEXT_DAY = DAY + 1
CHANNELS = {"c_1": "search", "c_2": "social"}


def session(person="u1", day=DAY, purchases=0, revenue=0, events_count=1):
    return events.Session(
        person, day * 86_400_000, day, events_count, revenue, purchases, None
    )


def attribution(
    ses, campaign="c_1", channel="search", assists=(), click_id=None
):
    return clicks.SessionAttribution(
        ses, campaign, channel, click_id, None, tuple(assists)
    )


class CampaignDailyTest(unittest.TestCase):
    def build(self, attributions):
        builder = campaign_daily.CampaignDailyBuilder()
        for item in attributions:
            builder.add(item)
        return builder.rows(CHANNELS)

    def test_credited_session_with_purchase(self):
        rows = self.build(
            [attribution(session(purchases=1, revenue=1000), assists=("c_1",))]
        )
        self.assertEqual(
            rows,
            [campaign_daily.CampaignDaily("c_1", "search", DAY, 1, 1, 1000, 1)],
        )

    def test_two_purchases_in_one_session_count_one_purchasing_session(self):
        rows = self.build(
            [attribution(session(events_count=3, purchases=2, revenue=1500))]
        )
        row = rows[0]
        self.assertEqual(row.sessions, 1)
        self.assertEqual(row.purchasing_sessions, 1)
        self.assertEqual(row.revenue_cents, 1500)

    def test_sessions_without_purchase_do_not_count_as_purchasing(self):
        rows = self.build(
            [attribution(session(purchases=0, revenue=0))]
        )
        self.assertEqual(rows[0].sessions, 1)
        self.assertEqual(rows[0].purchasing_sessions, 0)
        self.assertEqual(rows[0].revenue_cents, 0)

    def test_sessions_and_revenue_accumulate_per_day(self):
        rows = self.build(
            [
                attribution(session(purchases=1, revenue=300)),
                attribution(session(purchases=0, revenue=0)),
                attribution(session(day=NEXT_DAY, purchases=1, revenue=700)),
            ]
        )
        self.assertEqual(
            rows,
            [
                campaign_daily.CampaignDaily("c_1", "search", DAY, 2, 1, 300, 0),
                campaign_daily.CampaignDaily(
                    "c_1", "search", NEXT_DAY, 1, 1, 700, 0
                ),
            ],
        )

    def test_each_assisting_campaign_counts_once_per_session(self):
        rows = self.build(
            [attribution(session(), assists=("c_1", "c_2"))]
        )
        by_campaign = {row.campaign_id: row for row in rows}
        self.assertEqual(by_campaign["c_1"].assisted_sessions, 1)
        self.assertEqual(by_campaign["c_2"].assisted_sessions, 1)
        self.assertEqual(by_campaign["c_2"].sessions, 0)

    def test_credited_campaign_assists_its_own_session(self):
        rows = self.build([attribution(session(), assists=("c_1",))])
        self.assertEqual(rows[0].assisted_sessions, 1)

    def test_assisted_only_campaign_has_zero_credited_columns(self):
        rows = self.build(
            [attribution(session(purchases=1, revenue=500), assists=("c_2",))]
        )
        by_campaign = {row.campaign_id: row for row in rows}
        self.assertEqual(
            by_campaign["c_1"],
            campaign_daily.CampaignDaily("c_1", "search", DAY, 1, 1, 500, 0),
        )
        self.assertEqual(
            by_campaign["c_2"],
            campaign_daily.CampaignDaily("c_2", "social", DAY, 0, 0, 0, 1),
        )

    def test_organic_row_and_channel(self):
        rows = self.build(
            [attribution(session(), campaign="organic", channel="organic")]
        )
        self.assertEqual(
            rows,
            [campaign_daily.CampaignDaily("organic", "organic", DAY, 1, 0, 0, 0)],
        )

    def test_organic_never_assists(self):
        rows = self.build(
            [
                attribution(
                    session(),
                    campaign="organic",
                    channel="organic",
                    assists=("organic",),
                )
            ]
        )
        self.assertEqual(rows[0].assisted_sessions, 0)
        self.assertEqual(len(rows), 1)

    def test_rows_are_sorted_by_campaign_then_day(self):
        rows = self.build(
            [
                attribution(session(day=NEXT_DAY)),
                attribution(session(day=DAY)),
                attribution(session(), campaign="c_2", channel="social"),
            ]
        )
        self.assertEqual(
            [(row.campaign_id, row.day) for row in rows],
            [("c_1", DAY), ("c_1", NEXT_DAY), ("c_2", DAY)],
        )


if __name__ == "__main__":
    unittest.main()
