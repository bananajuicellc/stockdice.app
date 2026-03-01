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

import stockdice.config
import stockdice.db


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Initialize the users database (separate from financial data database)"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        default=False,
        help="WARNING: This will delete all user data! Drop and recreate user tables.",
    )
    args = parser.parse_args()

    if args.reset:
        print("WARNING: This will delete all user data!")
        response = input("Are you sure you want to continue? (yes/no): ")
        if response.lower() != "yes":
            print("Aborted.")
            exit(0)

    stockdice.db.create_all_user_tables(stockdice.config.config.users_db, reset=args.reset)
    print("Users database initialized successfully.")
