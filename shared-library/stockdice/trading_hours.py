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

import datetime
import zoneinfo

NEW_YORK = zoneinfo.ZoneInfo("America/New_York")
NEW_YORK_START_TIME = datetime.time(9, 30)
NEW_YORK_END_TIME = datetime.time(16, 0)


def is_new_york_regular_trading_hours():
    now = datetime.datetime.now(NEW_YORK)

    # Monday is 0, ..., Saturday is 5, Sunday is 6.
    if now.weekday() > 4:
        return False
    
    start_time = datetime.datetime(now.year, now.month, now.day, NEW_YORK_START_TIME.hour, NEW_YORK_START_TIME.minute, tzinfo=NEW_YORK)
    end_time = datetime.datetime(now.year, now.month, now.day, NEW_YORK_END_TIME.hour, NEW_YORK_END_TIME.minute, tzinfo=NEW_YORK)

    # TODO: Do we want to include logic for holidays?
    return start_time <= now <= end_time
