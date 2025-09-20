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

import freezegun
import pytest

import stockdice.trading_hours


@pytest.mark.parametrize(
    ("fake_now", "expected"),
    (
        pytest.param(
            datetime.datetime(
                2025, 5, 13, 22, 2, tzinfo=stockdice.trading_hours.NEW_YORK
            ),
            False,
            id="tuesday-after-hours",
        ),
        pytest.param(
            datetime.datetime(
                2025, 5, 13, 12, 59, tzinfo=stockdice.trading_hours.NEW_YORK
            ),
            True,
            id="tuesday-during-hours",
        ),
    ),
)
def test_is_new_york_regular_trading_hours(fake_now, expected):
    with freezegun.freeze_time(fake_now):
        assert stockdice.trading_hours.is_new_york_regular_trading_hours() == expected


@pytest.mark.parametrize(
    ("fake_now", "expected"),
    (
        pytest.param(
            datetime.datetime(
                2025, 5, 13, 12, 59, tzinfo=stockdice.trading_hours.NEW_YORK
            ),
            0.0,
            id="tuesday-during-hours",
        ),
        pytest.param(
            datetime.datetime(
                2025, 5, 13, 22, 2, tzinfo=stockdice.trading_hours.NEW_YORK
            ),
            (
                datetime.datetime(
                    2025, 5, 14, 9, 30, tzinfo=stockdice.trading_hours.NEW_YORK
                )
                - datetime.datetime(
                    2025, 5, 13, 22, 2, tzinfo=stockdice.trading_hours.NEW_YORK
                )
            )
            / datetime.timedelta(seconds=1),
            id="tuesday-after-hours",
        ),
        pytest.param(
            datetime.datetime(
                2025, 5, 13, 7, 2, tzinfo=stockdice.trading_hours.NEW_YORK
            ),
            (2 * 60 + 28) * 60.0,
            id="tuesday-before-hours",
        ),
        pytest.param(
            datetime.datetime(
                2025, 5, 13, 9, 2, tzinfo=stockdice.trading_hours.NEW_YORK
            ),
            28 * 60.0,
            id="tuesday-before-same-hour",
        ),
        pytest.param(
            datetime.datetime(
                2025, 5, 16, 12, 59, tzinfo=stockdice.trading_hours.NEW_YORK
            ),
            0.0,
            id="friday-during-hours",
        ),
        pytest.param(
            datetime.datetime(
                2025, 5, 16, 22, 2, tzinfo=stockdice.trading_hours.NEW_YORK
            ),
            (
                datetime.datetime(
                    2025, 5, 19, 9, 30, tzinfo=stockdice.trading_hours.NEW_YORK
                )
                - datetime.datetime(
                    2025, 5, 16, 22, 2, tzinfo=stockdice.trading_hours.NEW_YORK
                )
            )
            / datetime.timedelta(seconds=1),
            id="friday-after-hours",
        ),
        pytest.param(
            datetime.datetime(
                2025, 5, 16, 7, 2, tzinfo=stockdice.trading_hours.NEW_YORK
            ),
            (2 * 60 + 28) * 60.0,
            id="friday-before-hours",
        ),
        pytest.param(
            datetime.datetime(
                2025, 5, 16, 9, 2, tzinfo=stockdice.trading_hours.NEW_YORK
            ),
            28 * 60.0,
            id="friday-before-same-hour",
        ),
    ),
)
def test_seconds_to_next_new_york_trading_hours(fake_now, expected):
    with freezegun.freeze_time(fake_now):
        assert (
            stockdice.trading_hours.seconds_to_next_new_york_trading_hours() == expected
        )
