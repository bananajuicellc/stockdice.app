# Copyright 2021 Banana Juice LLC
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
import re


TIMEDELTA_REGEX = re.compile(
    r"^(?P<length>[0-9]+)(?P<units>w|d|h|s|ms|us)$"
)
TIMEDELTA_UNITS = {
    "w": "weeks",
    "d": "days",
    "h": "hours",
    # Intentionally omitting minutes since it could be ambiguous with months.
    "s": "seconds",
    "ms": "milliseconds",
    "us": "microseconds",
}


def parse_timedelta(value: str) -> datetime.timedelta:
    parsed = TIMEDELTA_REGEX.match(value)
    if not parsed:
        raise ValueError(r"Invalid timedelta: {value}")
    groups = parsed.groupdict()
    length = int(groups["length"])
    units = groups["units"]
    kwargs = {
        TIMEDELTA_UNITS[units]: length,
    }
    return datetime.timedelta(**kwargs)
