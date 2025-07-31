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

import argparse
import asyncio
import datetime

import stockdice.timeutils


async def main(*, max_age: datetime.timedelta = datetime.timedelta(days=1)):
    # Pseudocode:
    # Download quote in a loop.
    # In a background thread, every 5 minutes or so, copy a backup to GCS
    # When we're outside of trading hours, also download balance-sheet and income.

    download_symbol_directory.main()

    await asyncio.gather(
        download_forex.main(max_age=max_age),
        download_values.main(command="quote", max_age=max_age),
        download_values.main(command="balance-sheet", max_age=max_age),
        download_values.main(command="income", max_age=max_age),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-age", default="1d")
    args = parser.parse_args()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    max_age = stockdice.timeutils.parse_timedelta(args.max_age)
    loop.run_until_complete(main(max_age=max_age))
