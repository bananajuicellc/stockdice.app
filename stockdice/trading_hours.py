# Copyright 2025 Banana Juice LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import datetime
import zoneinfo

NEW_YORK = zoneinfo.ZoneInfo("America/New_York")
NEW_YORK_START_TIME = datetime.time(9, 30)
NEW_YORK_END_TIME = datetime.time(16, 0)


def is_new_york_regular_trading_hours(now: datetime.datetime | None = None) -> bool:
    if now is None:
        now = datetime.datetime.now(NEW_YORK)

    # Monday is 0, ..., Saturday is 5, Sunday is 6.
    if now.weekday() > 4:
        return False

    start_time = datetime.datetime(
        now.year,
        now.month,
        now.day,
        NEW_YORK_START_TIME.hour,
        NEW_YORK_START_TIME.minute,
        tzinfo=NEW_YORK,
    )
    end_time = datetime.datetime(
        now.year,
        now.month,
        now.day,
        NEW_YORK_END_TIME.hour,
        NEW_YORK_END_TIME.minute,
        tzinfo=NEW_YORK,
    )

    # TODO: Do we want to include logic for holidays?
    return start_time <= now <= end_time


def _days_to_next_trading(weekday: int):
    # Monday is 0, ..., Saturday is 5, Sunday is 6.
    if weekday < 4:
        return 1
    return 7 - weekday


def _is_before_regular_trading_hours(now: datetime.datetime):
    if now.weekday() > 4:
        return False

    if now.hour < NEW_YORK_START_TIME.hour:
        return True

    if now.hour > NEW_YORK_START_TIME.hour:
        return False

    return now.minute <= NEW_YORK_START_TIME.minute


def seconds_to_next_new_york_trading_hours() -> float:
    now = datetime.datetime.now(NEW_YORK)

    if is_new_york_regular_trading_hours(now):
        return 0

    if _is_before_regular_trading_hours(now):
        next_trading_day = now
    else:
        days = _days_to_next_trading(now.weekday())
        next_trading_day = now + datetime.timedelta(days=days)

    start_of_trading = datetime.datetime(
        year=next_trading_day.year,
        month=next_trading_day.month,
        day=next_trading_day.day,
        hour=NEW_YORK_START_TIME.hour,
        minute=NEW_YORK_START_TIME.minute,
        tzinfo=NEW_YORK,
    )
    return (start_of_trading - now) / datetime.timedelta(seconds=1)
