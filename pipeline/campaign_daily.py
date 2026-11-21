"""``campaign_daily`` aggregation from session attributions (rules.md, "Outputs").

One row per ``(campaign_id, day)`` with at least one session credited to or
assisted by the campaign:

* ``sessions`` -- sessions credited to the campaign that started that day;
* ``purchasing_sessions`` -- of those, sessions with at least one purchase;
* ``revenue_cents`` -- summed purchase price in those credited sessions;
* ``assisted_sessions`` -- sessions starting that day this campaign assisted.

A campaign that only assisted has a row with the three credited columns at zero.
``organic`` is used as a literal campaign/channel when no click qualified and
never assists.

The builder is a consumer for :func:`pipeline.clicks.stream_attributions`: it
folds each attribution into two small dictionaries keyed by ``(campaign, day)``
(one entry per output row, not per session or per click) and stores nothing
else. That is what lets the pipeline attribute 10M+ sessions without retaining
a parallel list of attribution objects.
"""
from __future__ import annotations

from typing import Dict, List, NamedTuple, Tuple

__all__ = ["ORGANIC", "CampaignDaily", "CampaignDailyBuilder"]

#: Campaign and channel used when no click qualifies; never in campaigns.csv.
ORGANIC = "organic"


class CampaignDaily(NamedTuple):
    campaign_id: str
    channel: str
    day: int  # days since 1970-01-01, UTC
    sessions: int
    purchasing_sessions: int
    revenue_cents: int
    assisted_sessions: int


class CampaignDailyBuilder:
    """Fold streamed attributions into ``(campaign, day)`` counters."""

    __slots__ = ("_credited", "_assisted")

    def __init__(self) -> None:
        # (campaign, day) -> [sessions, purchasing_sessions, revenue_cents]
        self._credited: Dict[Tuple[str, int], List[int]] = {}
        # (campaign, day) -> sessions assisted that day
        self._assisted: Dict[Tuple[str, int], int] = {}

    def add(self, attribution) -> None:
        """Consume one ``clicks.SessionAttribution``; nothing is retained."""
        session = attribution.session
        key = (attribution.campaign_id, session.day)
        aggregate = self._credited.get(key)
        if aggregate is None:
            aggregate = [0, 0, 0]
            self._credited[key] = aggregate
        aggregate[0] += 1
        if session.purchases:
            # A session with two purchases is still one purchasing session.
            aggregate[1] += 1
        aggregate[2] += session.revenue_cents

        for campaign in attribution.assists:
            # Only campaigns from campaigns.csv pass validation, so this is
            # belt-and-braces on the rule that organic never assists.
            if campaign == ORGANIC:
                continue
            assist_key = (campaign, session.day)
            self._assisted[assist_key] = self._assisted.get(assist_key, 0) + 1

    def rows(self, campaign_channels: Dict[str, str]) -> List[CampaignDaily]:
        """Materialize output rows, sorted by (campaign_id, day)."""
        rows: List[CampaignDaily] = []
        for key, aggregate in self._credited.items():
            campaign, day = key
            channel = (
                ORGANIC if campaign == ORGANIC else campaign_channels[campaign]
            )
            rows.append(
                CampaignDaily(
                    campaign,
                    channel,
                    day,
                    aggregate[0],
                    aggregate[1],
                    aggregate[2],
                    self._assisted.get(key, 0),
                )
            )
        for key, assisted in self._assisted.items():
            if key in self._credited:
                continue
            campaign, day = key
            rows.append(
                CampaignDaily(
                    campaign,
                    campaign_channels[campaign],
                    day,
                    0,
                    0,
                    0,
                    assisted,
                )
            )
        rows.sort(key=_key)
        return rows


def _key(row: CampaignDaily) -> Tuple[str, int]:
    return (row.campaign_id, row.day)
