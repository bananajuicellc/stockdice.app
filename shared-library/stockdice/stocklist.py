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

from __future__ import annotations

import argparse
import asyncio
import datetime
import logging

import aiohttp
import stockdice.ratelimits
import stockdice.config
import stockdice.timeutils


# Only include companies for whom financial statements are available.
# https://site.financialmodelingprep.com/developer/docs/stable/financial-symbols-list
FMP_FINANCIAL_STATEMENT_SYMBOL_LIST = "https://financialmodelingprep.com/stable/financial-statement-symbol-list?apikey={apikey}"


@stockdice.ratelimits.retry_fmp
async def download_symbol_list(*, session):
    db = stockdice.config.DB
    url = FMP_FINANCIAL_STATEMENT_SYMBOL_LIST.format(apikey=stockdice.config.FMP_API_KEY)
    last_updated_us = stockdice.timeutils.now_in_microseconds()

    async with session.get(url) as resp:
        resp_json = await stockdice.ratelimits.check_status(resp)

        db.executemany(
            f"""INSERT INTO symbol
            (symbol, company_name, trading_currency, reporting_currency, last_updated_us)
            VALUES (:symbol, :companyName, :tradingCurrency, :reportingCurrency, {last_updated_us})
            ON CONFLICT(symbol) DO UPDATE
            SET company_name=excluded.company_name,
            trading_currency = excluded.trading_currency,
            reporting_currency = excluded.trading_currency,
            last_updated_us = excluded.last_updated_us;
            """,
            resp_json,
        )
        db.execute(f"DELETE FROM symbol WHERE last_updated_us <> {last_updated_us};")
        db.commit()
