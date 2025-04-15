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

import asyncio
import logging

import aiohttp

from helpers import *


FMP_FOREX_LIST = "https://financialmodelingprep.com/stable/forex-list?apikey={apikey}"
FMP_FOREX_QUOTE = "https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={apikey}"


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
    url = FMP_FOREX_QUOTE.format(symbol, apikey=FMP_API_KEY)
    async with session.get(url) as resp:
        resp_json = await check_status(resp)
        market_cap = None
        if resp_json:
            market_cap = resp_json[0].get("marketCap")
        if market_cap is None:
            logging.warning(f"no market cap for {symbol}")
            market_cap = 0

        DB.execute(
            """INSERT INTO forex
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


async def download_pairs(session, out):
    for currency in CURRENCIES:
        await download_pair(session, out, currency)


async def main():
    async with aiohttp.ClientSession() as session:
        for symbol in await download_forex_list(session):
            print(symbol)

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
