#!/usr/bin/env python
# coding: utf-8
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

"""Refreshes the DB constantly and creates backups to GCS every few minutes."""

from __future__ import annotations

import argparse
import asyncio
import datetime
import logging
import sqlite3
import sys
import threading
import time

import httpx
from google.cloud import storage

import stockdice.config
import stockdice.balance_sheet
import stockdice.company_profile
import stockdice.forex
import stockdice.income
import stockdice.stocklist
import stockdice.trading_hours


# TODO: Where should I be configuring logging?
# https://stackoverflow.com/a/14058475/101923
root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)

# It can take about 1 hour to refresh, so include a max age about that long so
# we can save time if the task has to restart.
MAX_AGE = datetime.timedelta(minutes=60)


def backup_db():
    db = sqlite3.connect(
        stockdice.config.DB_PATH,
        autocommit=False,
    )

    # End the transaction that was started automatically.
    db.executescript("ROLLBACK;")

    # Enable Write-Ahead Logging for greater concurrency.
    # https://stackoverflow.com/a/39265148/101923
    db.execute("PRAGMA journal_mode=WAL")

    backup_path = stockdice.config.DB_REPLICA_PATH
    bucket_name = stockdice.config.config.bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(backup_path.name)

    while True:
        try:
            backup_path.unlink()
        except FileNotFoundError:
            # This is expected on the first run.
            pass

        db.executescript(
            f"""
            VACUUM main INTO
            {repr(str((backup_path).absolute()))};
            """
        )
        blob.upload_from_filename(backup_path)
        time.sleep(stockdice.config.config.backup_interval_seconds)


def backup_db_loop():
    """Infinite loop in case backup fails."""
    while True:
        try:
            backup_db()
        except Exception:
            logging.exception("Got exception in backup_db thread.")


async def download_all(*, client: httpx.AsyncClient):
    await stockdice.stocklist.download_symbol_list(client=client)

    await asyncio.gather(
        stockdice.forex.download_forex(max_age=MAX_AGE, client=client),
        stockdice.company_profile.download_all(max_age=MAX_AGE, client=client),
        stockdice.income.download_all(max_age=MAX_AGE, client=client),
        stockdice.balance_sheet.download_all(max_age=MAX_AGE, client=client),
    )


async def download_market_data(*, client: httpx.AsyncClient):
    """During market hours, just download data that changes more frequently."""
    await stockdice.stocklist.download_symbol_list(client=client)

    await asyncio.gather(
        stockdice.forex.download_forex(max_age=MAX_AGE, client=client),
        stockdice.company_profile.download_all(max_age=MAX_AGE, client=client),
    )


async def main():
    backup_thread = threading.Thread(target=backup_db, daemon=True)
    backup_thread.start()

    async with httpx.AsyncClient() as client:
        while True:
            # Prioritize market data during trading hours, since that changes
            # much more quickly.
            if stockdice.trading_hours.is_new_york_regular_trading_hours():
                await download_market_data(client=client)
            else:
                await download_all(client=client)

                sleep_seconds = stockdice.trading_hours.seconds_to_next_new_york_trading_hours()
                logging.info(f"Outside of trading hours. Sleeping for {sleep_seconds / 60 / 60} hours.")
                await asyncio.sleep(sleep_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
