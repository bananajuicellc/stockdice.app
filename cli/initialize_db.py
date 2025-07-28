#!/usr/bin/env python
# coding: utf-8
# Copyright 2023 Banana Juice LLC
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
import sys

import stockdice.db


def load_quote(quote_path):
    quote = pandas.read_csv(quote_path, header=None, names=["symbol", "market_cap_usd"], index_col="symbol")
    quote = quote.groupby(quote.index).last()
    quote.to_sql("quotes", DB, if_exists="append")


def load_balance_sheet(balance_sheet_path):
    balance_sheet = pandas.read_csv(
        balance_sheet_path,
        header=None,
        names=["symbol", "book", "currency"],
        index_col="symbol",
    )
    balance_sheet = balance_sheet.groupby(balance_sheet.index).last()
    balance_sheet.to_sql("balance_sheets", DB, if_exists="append")


def load_income(income_path):
    income = pandas.read_csv(
        income_path,
        header=None,
        names=["symbol", "profit", "revenue", "currency"],
        index_col="symbol",
    )
    income = income.groupby(income.index).last()
    income.to_sql("incomes", DB, if_exists="append")


if __name__ == "__main__":
    import stockdice.config
    import stockdice.db

    stockdice.db.create_all_tables(stockdice.config.DB)
