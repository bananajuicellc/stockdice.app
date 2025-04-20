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

import datetime
import logging
import pathlib
import sqlite3

import aiohttp
import toml


FMP_FOREX_LIST = "https://financialmodelingprep.com/stable/forex-list?apikey={apikey}"
FMP_FOREX_QUOTE = "https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={apikey}"

DIR = pathlib.Path(__file__).parent
DB = sqlite3.connect(DIR / "third_party" / "financialmodelingprep.com" / "stockdice.sqlite")
NASDAQ_DIR = DIR / "third_party" / "ftp.nasdaqtrader.com"
FMP_DIR = DIR / "third_party" / "financialmodelingprep.com"

with open(DIR / "environment.toml") as config_file:
    config = toml.load(config_file)

FMP_API_KEY = config["FMP_API_KEY"]

forex_to_usd = None


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


@retry_fmp
async def download_forex_list(session):
    url = FMP_FOREX_LIST.format(apikey=FMP_API_KEY)
    symbols = []

    async with session.get(url) as resp:
        resp_json = await check_status(resp)
        for forex in resp_json:
            symbol = forex.get("symbol")
            from_currency = forex.get("fromCurrency")
            to_currency = forex.get("toCurrency")
            from_name = forex.get("fromName")
            to_name = forex.get("toName")

            if to_currency is None or to_currency.upper() != "USD":
                continue

            DB.execute(
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
            DB.commit()
            symbols.append(symbol)

    return symbols


@retry_fmp
async def download_forex_quote(session, symbol: str, last_updated_us: int):
    url = FMP_FOREX_QUOTE.format(symbol=symbol, apikey=FMP_API_KEY)
    async with session.get(url) as resp:
        resp_json = await check_status(resp)
        if resp_json:
            price = resp_json[0].get("price")

        if price is None:
            logging.warning(f"no price for {symbol}")
        else:
            price = float(price)

        DB.execute(
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
                "last_updated_us": last_updated_us,
            },
        )
        DB.commit()


async def main(*, max_age: datetime.timedelta = datetime.timedelta(days=1)):
    async with aiohttp.ClientSession() as session:
        all_symbols = await download_forex_list(session)
        return await download_all(
            download_forex_quote,
            "forex",
            max_age=max_age,
            all_symbols=all_symbols,
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
