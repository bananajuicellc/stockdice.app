#!/usr/bin/env python
# coding: utf-8
# Copyright 2018 Banana Juice LLC
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
import dataclasses
import io

import numpy
import pandas

import stockdice.config


@dataclasses.dataclass
class Tables:
    balance_sheet: pandas.DataFrame
    company_profile: pandas.DataFrame
    income: pandas.DataFrame
    forex: pandas.DataFrame
    symbols: pandas.DataFrame


def load_dfs() -> Tables:
    balance_sheet = pandas.read_sql(
        """
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
        """,
        stockdice.config.DB,
    )
    company_profile = pandas.read_sql(
        """
        SELECT symbol, price, marketCap, currency
        FROM company_profile
        WHERE isEtf = false
        AND isFund = false;
        """,
        stockdice.config.DB,
    )
    forex = pandas.read_sql(
        """
        SELECT symbol, from_currency, to_currency, price
        FROM forex;
        """,
        stockdice.config.DB,
    )
    income = pandas.read_sql(
        """
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
        """,
        stockdice.config.DB,
    )
    symbols = pandas.read_sql(
        "SELECT symbol, company_name FROM symbol;",
        stockdice.config.DB,
    )
    return Tables(
        balance_sheet=balance_sheet,
        company_profile=company_profile,
        income=income,
        forex=forex,
        symbols=symbols,
    )


def add_usd_column_from_forex(df, column):
    df[f"usd_{column}"] = df.apply(
        lambda row: helpers.to_usd(row["currency"], row[column]),
        axis=1,
    )


def output_dataframe(result, output_path, format):
    if format == "csv":
        if output_path == "--":
            with io.StringIO() as out:
                result.to_csv(out, index=False)
                print(out.getvalue())
        else:
            result.to_csv(output_path, index=False)
    elif format == "text":
        if output_path != "--":
            raise ValueError("text output to file not supported")
        print(result)


def main(number_of_rolls=1, output_path="--", format="csv"):
    all_symbols, quote, income, balance_sheet = load_dfs()
    add_usd_column_from_forex(income, "revenue")
    add_usd_column_from_forex(income, "profit")
    add_usd_column_from_forex(balance_sheet, "book")

    screen = all_symbols.merge(
        quote.merge(
            income.merge(
                balance_sheet,
                how="outer",
                on="symbol",
            ),
            how="outer",
            on="symbol",
        ),
        # The DB may have out-of-date symbols, use latest from NASDAQ.
        how="left",
    ).fillna(value=0)
    screen.drop_duplicates(keep="last")
    screen_ones = numpy.ones(len(screen.index))

    # Even weight seemed to skew too heavily towards value. Place a more weight in
    # market cap, since market risk is the main factor I want to target.
    screen["average"] = numpy.exp(
        (1.0 / 10.0)
        * (
            1 * numpy.log(numpy.fmax(screen_ones, screen["usd_book"]))
            + 2 * numpy.log(numpy.fmax(screen_ones, screen["usd_profit"]))
            + 2 * numpy.log(numpy.fmax(screen_ones, screen["usd_revenue"]))
            + 5 * numpy.log(numpy.fmax(screen_ones, screen["market_cap"]))
        )
    )

    result = screen.sample(n=number_of_rolls, weights=screen["average"], replace=True)
    output_dataframe(result, output_path, format)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="stockdice.py")
    parser.add_argument("-n", "--number", type=int, default=1)
    parser.add_argument("-o", "--output", default="--")
    parser.add_argument("-f", "--format", default="csv")
    args = parser.parse_args()
    main(number_of_rolls=args.number, output_path=args.output, format=args.format)
