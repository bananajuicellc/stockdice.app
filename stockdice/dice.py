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

import dataclasses
import sqlite3

import numpy.random
import polars

import stockdice.config


@dataclasses.dataclass
class _Tables:
    company_profile: polars.DataFrame
    most_recent_fy_balance_sheet: polars.DataFrame
    most_recent_fy_income: polars.DataFrame
    forex: polars.DataFrame


def _load_dfs() -> _Tables:
    # TODO: we might be able to use read_database_uri if we're more careful
    # about what types we store in each column.
    db = sqlite3.connect(stockdice.config.DB_REPLICA_PATH.absolute())
    # Enable Write-Ahead Logging for greater concurrency.
    # https://stackoverflow.com/a/39265148/101923
    db.execute("PRAGMA journal_mode=WAL")
    company_profile_query = """
        SELECT *
        FROM company_profile
        WHERE isEtf = false
        AND isFund = false;
        """
    company_profile = polars.read_database(
        query=company_profile_query, connection=db
    )
    forex = polars.read_database(
        query="SELECT * FROM forex WHERE to_currency = 'USD';",
        connection=db,
    )
    balance_sheet_query = """
        SELECT
            symbol,
            fiscalYear,
            period,
            date,
            reportedCurrency,
            totalAssets,
            totalLiabilities,
            last_updated_us
        FROM (
            SELECT
                symbol,
                fiscalYear,
                period,
                date,
                reportedCurrency,
                totalAssets,
                totalLiabilities,
                last_updated_us,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol
                    ORDER BY fiscalYear DESC
                ) as rn
            FROM
                balance_sheet
            WHERE
                -- TODO: how to handle quarterly reports?
                period = 'FY'
        )
        WHERE
            rn = 1;
        """
    most_recent_fy_balance_sheet = polars.read_database(
        query=balance_sheet_query, connection=db
    )
    income_query = """
        SELECT
            symbol,
            fiscalYear,
            period,
            date,
            reportedCurrency,
            revenue,
            netIncome,
            last_updated_us
        FROM (
            SELECT
                symbol,
                fiscalYear,
                period,
                date,
                reportedCurrency,
                revenue,
                netIncome,
                last_updated_us,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol
                    ORDER BY fiscalYear DESC
                ) as rn
            FROM
                income
            WHERE
                -- TODO: how to handle quarterly reports?
                period = 'FY'
        )
        WHERE
            rn = 1;
        """
    most_recent_fy_income = polars.read_database(
        query=income_query, connection=db
    )
    return _Tables(
        company_profile=company_profile,
        forex=forex,
        most_recent_fy_balance_sheet=most_recent_fy_balance_sheet,
        most_recent_fy_income=most_recent_fy_income,
    )


def roll(*, n: int = 1, weights=None) -> polars.DataFrame:
    dfs = _load_dfs()

    # Convert currencies to USD
    company_profile_usd = (
        dfs.company_profile.join(
            dfs.forex, left_on="currency", right_on="from_currency", suffix="_forex"
        )
        .select(
            polars.col("symbol"),
            polars.col("companyName"),
            marketCapUSD=polars.col("marketCap") * polars.col("price_forex"),
        )
        .filter(
            polars.col("marketCapUSD") > 0
        )
        .with_row_index("idx")
    )

    if weights is None:
        samples = company_profile_usd.sample(n=n)
    else:
        # Inspired by the numpy implementation of weighted sampling with replacement:
        # https://github.com/numpy/numpy/blob/f5a6af86acab7fcd1644fad76b5fbe466a0a98dd/numpy/random/mtrand.pyx#L1009-L1015
        # TODO: support more than just market cap weighted.
        pdf = (
            company_profile_usd["marketCapUSD"]
            / company_profile_usd["marketCapUSD"].sum()
        )
        cdf = pdf.cum_sum()
        rng = numpy.random.default_rng()
        targets = rng.uniform(0, 1, size=(n,))
        sample_idxs = cdf.search_sorted(targets).to_frame("idx")
        samples = company_profile_usd.join(sample_idxs, on="idx")

    return samples.select(
        polars.col("symbol"), polars.col("companyName"), polars.col("marketCapUSD")
    )
