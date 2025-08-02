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
import functools
import random
import time

import httpx

import stockdice.config


# Rate limit from our side. We only want to download BATCH_SIZE records per
# BATCH_WAIT seconds. At 300 API calls / minute, we can do at most 5 per second.
SECONDS_BETWEEN_REQUESTS = 60.0 / stockdice.config.REQUESTS_PER_MINUTE
next_request_time = time.monotonic()
request_lock = asyncio.Lock()

# Rate limit from server side. This is especially useful when we're downloading
# from several APIs at once.
RATE_LIMIT_STATUS = 429
RATE_LIMIT_SECONDS = "X-Rate-Limit-Retry-After-Seconds"
RATE_LIMIT_MILLISECONDS = "X-Rate-Limit-Retry-After-Milliseconds"


class RateLimitError(Exception):
    def __init__(self, seconds, millis):
        self.seconds = seconds
        self.millis = millis


async def get(client: httpx.AsyncClient, url: str):
    global next_request_time

    async with request_lock:
        current_time = time.monotonic()
        if current_time < next_request_time:
            await asyncio.sleep(next_request_time - current_time)

        next_request_time = current_time + SECONDS_BETWEEN_REQUESTS
        return await client.get(url)


def check_status(resp):
    if resp.status_code == RATE_LIMIT_STATUS:
        raise RateLimitError(1, 0)
    resp_json = resp.json()
    if RATE_LIMIT_SECONDS in resp_json or RATE_LIMIT_MILLISECONDS in resp_json:
        raise RateLimitError(
            float(resp_json.get(RATE_LIMIT_SECONDS, 0)),
            float(resp_json.get(RATE_LIMIT_MILLISECONDS, 0)),
        )
    return resp_json


def retry_fmp(async_fn):
    @functools.wraps(async_fn)
    async def wrapped(*args, **kwargs):
        global next_request_time

        while True:
            try:
                value = await async_fn(*args, **kwargs)
            except RateLimitError as exp:
                sleep_seconds = exp.seconds + (exp.millis / 1000.0)
                jitter = random.random()
                current_time = time.monotonic()
                # Should be OK without a lock because variable assignment is atomic in Python.
                next_request_time = current_time + sleep_seconds + jitter
            except httpx.ReadTimeout:
                # Try again.
                pass
            except:
                raise
            else:
                return value

    return wrapped
