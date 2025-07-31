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

import argparse
import asyncio
import datetime
import logging

import httpx

import stockdice.ratelimits
import stockdice.config
import stockdice.timeutils


FMP_FOREX_LIST = "https://financialmodelingprep.com/stable/forex-list?apikey={apikey}"
FMP_FOREX_QUOTE = (
    "https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={apikey}"
)

forex_to_usd = None


def load_forex():
    global forex_to_usd
    forex_to_usd = {"USD": 1.0}

    db = stockdice.config.DB
    rows = db.execute(
        """
        SELECT from_currency, price
        FROM forex
        WHERE to_currency = 'USD'
        """
    )
    for row in rows:
        from_currency, price = row
        forex_to_usd[from_currency] = price


def to_usd(*, curr, value):
    if forex_to_usd is None:
        load_forex()
    if curr is None or curr != curr or curr in {"None", "unknown"}:
        # Assume USD? None usually corresponds to no reported value.
        return value

    return forex_to_usd[curr] * value


async def download_forex(
    *,
    max_age: datetime.timedelta = datetime.timedelta(days=1),
    client: httpx.AsyncClient,
):
    all_symbols = await download_forex_list(client=client)
    return await asyncio.gather(
        *[
            download_forex_quote(client=client, symbol=symbol, max_age=max_age)
            for symbol in all_symbols
        ]
    )


@stockdice.ratelimits.retry_fmp
async def download_forex_list(*, client: httpx.AsyncClient):
    db = stockdice.config.DB
    url = FMP_FOREX_LIST.format(apikey=stockdice.config.FMP_API_KEY)
    symbols = []

    resp = await stockdice.ratelimits.get(client, url)
    resp_json = stockdice.ratelimits.check_status(resp)
    for forex in resp_json:
        symbol = forex.get("symbol")
        from_currency = forex.get("fromCurrency")
        to_currency = forex.get("toCurrency")
        from_name = forex.get("fromName")
        to_name = forex.get("toName")

        if to_currency is None or to_currency.upper() != "USD":
            continue

        db.execute(
            """INSERT INTO forex
            (symbol, from_currency, to_currency, from_name, to_name)
            VALUES (:symbol, :from_currency, :to_currency, :from_name, :to_name)
            ON CONFLICT(symbol) DO UPDATE
            SET from_currency=excluded.from_currency,
            to_currency=excluded.to_currency,
            from_name=excluded.from_name,
            to_name=excluded.to_name;
            """,
            {
                "symbol": symbol,
                "from_currency": from_currency,
                "to_currency": to_currency,
                "from_name": from_name,
                "to_name": to_name,
            },
        )
        db.commit()
        symbols.append(symbol)

    return symbols


@stockdice.ratelimits.retry_fmp
async def download_forex_quote(
    *, client: httpx.AsyncClient, symbol: str, max_age: datetime.timedelta
):
    db = stockdice.config.DB

    now_us = stockdice.timeutils.now_in_microseconds()
    last_updated = db.execute(
        "SELECT last_updated_us FROM forex WHERE symbol = :symbol", {"symbol": symbol}
    ).fetchone()
    if (
        last_updated
        and datetime.timedelta(microseconds=now_us - last_updated[0]) <= max_age
    ):
        logging.info(f"Data already fresh, skipping forex for {symbol}.")
        return

    url = FMP_FOREX_QUOTE.format(symbol=symbol, apikey=stockdice.config.FMP_API_KEY)
    resp = await stockdice.ratelimits.get(client, url)
    resp_json = stockdice.ratelimits.check_status(resp)
    if resp_json:
        price = resp_json[0].get("price")

    if price is None:
        logging.warning(f"no price for {symbol}")
    else:
        price = float(price)

    db.execute(
        """INSERT INTO forex
        (symbol, price, last_updated_us)
        VALUES (:symbol, :price, :last_updated_us)
        ON CONFLICT(symbol) DO UPDATE
        SET price=excluded.price,
            last_updated_us=excluded.last_updated_us
        """,
        {
            "symbol": symbol,
            "price": price,
            "last_updated_us": now_us,
        },
    )
    db.commit()


async def main(*, max_age: datetime.timedelta = datetime.timedelta(days=1)):
    async with httpx.AsyncClient() as client:
        return await download_forex(max_age=max_age, client=client)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-age", default="1d")
    args = parser.parse_args()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    max_age = stockdice.timeutils.parse_timedelta(args.max_age)
    loop.run_until_complete(main(max_age=max_age))
