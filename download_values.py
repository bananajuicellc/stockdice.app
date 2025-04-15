#!/usr/bin/env python
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

import argparse
import asyncio
import datetime
import logging
import time
import sys

import aiohttp

from helpers import *

from typing import Optional


FMP_QUOTE = "https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={apikey}"
FMP_INCOME_STATEMENT = "https://financialmodelingprep.com/api/v3/income-statement/{symbol}?limit=1&apikey={apikey}"
FMP_BALANCE_SHEET = "https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?period=quarter&limit=1&apikey={apikey}"


def load_symbols():
    all_symbols = []

    with open(NASDAQ_DIR / "allsymbols.txt", "r") as handle:
        for line in handle:
            all_symbols.append(line.strip())

    all_symbols.sort()
    return all_symbols


@retry_fmp
async def download_income(
    session, symbol: str, last_updated_us: int
):
    """
    https://www.ftserussell.com/research/factor-exposure-indexes-value-factor

    Earnings Yield, Cash Flow Yield and Sales to Price (most performance)
    """
    url = FMP_INCOME_STATEMENT.format(symbol=symbol, apikey=FMP_API_KEY)
    async with session.get(url) as resp:
        resp_json = await check_status(resp)
        profit = None
        revenue = None
        currency = None
        if resp_json:
            profit = resp_json[0].get("grossProfit")
            revenue = resp_json[0].get("revenue")
            currency = resp_json[0].get("reportedCurrency")
        if revenue is None:
            logging.warning(f"no revenue for {symbol}")
            revenue = 0
        if profit is None:
            logging.warning(f"no profit for {symbol}")
            profit = 0

        DB.execute(
            """INSERT INTO incomes
            (symbol, profit, revenue, currency, last_updated_us)
            VALUES (:symbol, :profit, :revenue, :currency, :last_updated_us)
            ON CONFLICT(symbol) DO UPDATE
            SET profit=excluded.profit,
              revenue=excluded.revenue,
              currency=excluded.currency,
              last_updated_us=excluded.last_updated_us
            """,
            {
                "symbol": symbol,
                "profit": float(profit),
                "revenue": float(revenue),
                "currency": currency,
                "last_updated_us": last_updated_us,
            },
        )
        DB.commit()


@retry_fmp
async def download_balance_sheet(session, symbol: str, last_updated_us: int):
    """
    https://codingandfun.com/how-to-calculate-price-book-ratio-with-python/

    https://www.ftserussell.com/research/factor-exposure-indexes-value-factor

    Book to Price (most diversified)
    """
    url = FMP_BALANCE_SHEET.format(symbol=symbol, apikey=FMP_API_KEY)
    async with session.get(url) as resp:
        resp_json = await check_status(resp)
        book_value = None
        currency = None
        if resp_json:
            book_value = resp_json[0].get("totalStockholdersEquity")
            currency = resp_json[0].get("reportedCurrency")
        if book_value is None:
            logging.warning(f"no book value for {symbol}")
            book_value = 0

        DB.execute(
            """INSERT INTO balance_sheets
            (symbol, book, currency, last_updated_us)
            VALUES (:symbol, :book, :currency, :last_updated_us)
            ON CONFLICT(symbol) DO UPDATE
            SET book=excluded.book,
              currency=excluded.currency,
              last_updated_us=excluded.last_updated_us
            """,
            {
                "symbol": symbol,
                "book": float(book_value),
                "currency": currency,
                "last_updated_us": last_updated_us,
            },
        )
        DB.commit()


@retry_fmp
async def download_market_cap(session, symbol: str, last_updated_us: int):
    url = FMP_QUOTE.format(symbol=symbol, apikey=FMP_API_KEY)
    async with session.get(url) as resp:
        resp_json = await check_status(resp)
        market_cap = None
        if resp_json:
            market_cap = resp_json[0].get("marketCap")
        if market_cap is None:
            logging.warning(f"no market cap for {symbol}")
            market_cap = 0

        DB.execute(
            """INSERT INTO quotes
            (symbol, market_cap_usd, last_updated_us)
            VALUES (:symbol, :market_cap_usd, :last_updated_us)
            ON CONFLICT(symbol) DO UPDATE
            SET market_cap_usd=excluded.market_cap_usd,
              last_updated_us=excluded.last_updated_us
            """,
            {
                "symbol": symbol,
                "market_cap_usd": float(market_cap),
                "last_updated_us": last_updated_us,
            },
        )
        DB.commit()


async def main(download_fn, table: str, max_age: datetime.timedelta = datetime.timedelta(days=1)):
    all_symbols = load_symbols()
    return download_all(download_fn, table, max_age=max_age, all_symbols=all_symbols)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-age", default="1d")
    parser.add_argument("command")
    args = parser.parse_args()
    command = args.command
    if command == "quote":
        table = "quotes"
        download_fn = download_market_cap
    elif command == "balance-sheet":
        table = "balance_sheets"
        download_fn = download_balance_sheet
    elif command == "income":
        table = "incomes"
        download_fn = download_income
    else:
        sys.exit("expected {quote,balance-sheet,income}")

    max_age = parse_timedelta(args.max_age)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(download_fn, table, max_age=max_age))
