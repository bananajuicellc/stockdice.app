# coding: utf-8
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

from __future__ import annotations

import asyncio
import datetime
import pathlib
import functools
import re
import sqlite3
import time
from typing import Iterable

import aiohttp
import toml


DIR = pathlib.Path(__file__).parent
DB = sqlite3.connect(DIR / "third_party" / "financialmodelingprep.com" / "stockdice.sqlite")
NASDAQ_DIR = DIR / "third_party" / "ftp.nasdaqtrader.com"
FMP_DIR = DIR / "third_party" / "financialmodelingprep.com"

with open(DIR / "environment.toml") as config_file:
    config = toml.load(config_file)

FMP_API_KEY = config["FMP_API_KEY"]

BATCH_SIZE = 10
BATCH_WAIT = 1
RATE_LIMIT_STATUS = 429
RATE_LIMIT_SECONDS = "X-Rate-Limit-Retry-After-Seconds"
RATE_LIMIT_MILLISECONDS = "X-Rate-Limit-Retry-After-Milliseconds"

forex_to_usd = None

class RateLimitError(Exception):
    def __init__(self, seconds, millis):
        self.seconds = seconds
        self.millis = millis


def retry_fmp(async_fn):
    @functools.wraps(async_fn)
    async def wrapped(*args):
        while True:
            try:
                value = await async_fn(*args)
            except RateLimitError as exp:
                await asyncio.sleep(exp.seconds + (exp.millis / 1000.0))
            except:
                raise
            else:
                return value

    return wrapped


async def check_status(resp):
    if resp.status == RATE_LIMIT_STATUS:
        raise RateLimitError(1, 0)
    resp_json = await resp.json()
    if RATE_LIMIT_SECONDS in resp_json or RATE_LIMIT_MILLISECONDS in resp_json:
        raise RateLimitError(
            float(resp_json.get(RATE_LIMIT_SECONDS, 0)),
            float(resp_json.get(RATE_LIMIT_MILLISECONDS, 0)),
        )
    return resp_json


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


def load_forex():
    global forex_to_usd
    forex_to_usd = {"USD": 1.0}

    rows = DB.execute(
        """
        SELECT from_currency, price
        FROM forex
        WHERE to_currency = 'USD'
        """
    )
    for row in rows:
        from_currency, price = row
        forex_to_usd[from_currency] = price


def to_usd(curr, value):
    if forex_to_usd is None:
        load_forex()
    if curr is None or curr != curr or curr in {"None", "unknown"}:
        # Assume USD? None usually corresponds to no reported value.
        return value

    return forex_to_usd[curr] * value


def is_fresh(table: str, symbol: str, max_last_updated_us: int) -> bool:
    cursor = DB.execute(
        f"SELECT last_updated_us FROM {table} WHERE symbol = :symbol", {"symbol": symbol}
    )
    previous_last_updated = cursor.fetchone()
    return (
        previous_last_updated is not None
        and previous_last_updated[0] is not None
        and previous_last_updated[0] > max_last_updated_us
    )


async def download_all(
    download_fn,
    table: str,
    *,
    max_age: datetime.timedelta = datetime.timedelta(days=1),
    all_symbols: Iterable[str],
):
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    now = datetime.datetime.now(datetime.timezone.utc)
    last_updated_us = (now - epoch) / datetime.timedelta(microseconds=1)
    # The oldest we'll allow a value to be before we have to refresh it.
    max_last_updated_us = ((now - max_age) - epoch) / datetime.timedelta(microseconds=1)

    async with aiohttp.ClientSession() as session:
        batch_index = 0
        batch_start = time.monotonic()
        for symbol in all_symbols:
            if is_fresh(table, symbol, max_last_updated_us):
                continue

            # Rate limit!
            if batch_index >= BATCH_SIZE:
                batch_time = time.monotonic() - batch_start
                remaining = BATCH_WAIT - batch_time
                if remaining > 0:
                    await asyncio.sleep(remaining)
                batch_start = time.monotonic()
                batch_index = 0
            await download_fn(
                session,
                symbol,
                last_updated_us,
            )


__all__ = [
    "DB",
    "DIR",
    "NASDAQ_DIR",
    "FMP_DIR",
    "FMP_API_KEY",
    "RateLimitError",
    "check_status",
    "parse_timedelta",
    "retry_fmp",
    "to_usd",
    "is_fresh",
    "download_all",
]
