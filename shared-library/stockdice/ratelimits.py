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

import asyncio
import datetime
import functools
import random
import time
from typing import Iterable

import aiohttp

import stockdice.db


# Rate limit from our side. We only want to download BATCH_SIZE records per
# BATCH_WAIT seconds. At 300 API calls / minute, we can do at most 5 per second.
BATCH_SIZE = 5
BATCH_WAIT = 1

# Rate limit from server side. This is especially useful when we're downloading
# from several APIs at once.
RATE_LIMIT_STATUS = 429
RATE_LIMIT_SECONDS = "X-Rate-Limit-Retry-After-Seconds"
RATE_LIMIT_MILLISECONDS = "X-Rate-Limit-Retry-After-Milliseconds"


class RateLimitError(Exception):
    def __init__(self, seconds, millis):
        self.seconds = seconds
        self.millis = millis


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


def retry_fmp(async_fn):
    @functools.wraps(async_fn)
    async def wrapped(*args, **kwargs):
        while True:
            try:
                value = await async_fn(*args, **kwargs)
            except RateLimitError as exp:
                sleep_seconds = exp.seconds + (exp.millis / 1000.0)
                jitter = random.randint(0, int(sleep_seconds) + 1)
                await asyncio.sleep(sleep_seconds + jitter)
            except:
                raise
            else:
                return value

    return wrapped


async def download_all(
    download_fn,
    *,
    table: str,
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
            if stockdice.db.is_fresh(table=table, symbol=symbol, max_last_updated_us=max_last_updated_us):
                continue

            # Rate limit! We only want to download BATCH_SIZE records per
            # BATCH_WAIT seconds.
            if batch_index >= BATCH_SIZE:
                batch_time = time.monotonic() - batch_start
                remaining = BATCH_WAIT - batch_time
                if remaining > 0:
                    await asyncio.sleep(remaining)
                batch_start = time.monotonic()
                batch_index = 0
            await download_fn(
                session=session,
                symbol=symbol,
                last_updated_us=last_updated_us,
            )
