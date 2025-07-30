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

import stockdice.config


def create_all_tables(db):
    create_balance_sheet(db)
    create_company_profile(db)
    create_forex(db)
    create_income(db)
    create_symbols(db)


def create_balance_sheet(db):
    db.execute("DROP TABLE IF EXISTS balance_sheets;")
    db.execute("""
    CREATE TABLE balance_sheets(
    symbol STRING PRIMARY KEY,
    book REAL,
    currency STRING,
    last_updated_us INTEGER
    );
    """)
    db.commit()


def create_forex(db):
    db.execute("DROP TABLE IF EXISTS forex;")
    db.execute("""
    CREATE TABLE forex(
    symbol STRING PRIMARY KEY,
    from_currency STRING,
    to_currency STRING,
    from_name STRING,
    to_name STRING,
    price REAL,
    last_updated_us INTEGER
    );
    """)
    db.commit()


def create_income(db):
    db.execute("DROP TABLE IF EXISTS incomes;")
    db.execute("""
    CREATE TABLE incomes(
    symbol STRING PRIMARY KEY,
    profit REAL,
    revenue REAL,
    currency STRING,
    last_updated_us INTEGER
    );
    """)
    db.commit()


def create_symbols(db):
    db.execute("DROP TABLE IF EXISTS symbol;")
    db.execute("""
    CREATE TABLE symbol(
    symbol STRING PRIMARY KEY,
    company_name STRING,
    trading_currency STRING,
    reporting_currency STRING,
    last_updated_us INTEGER
    );
    """)
    db.commit()


def create_company_profile(db):
    db.execute("DROP TABLE IF EXISTS company_profile;")
    db.execute(
        """
        CREATE TABLE company_profile (
            symbol TEXT PRIMARY KEY,
            price REAL,
            marketCap INTEGER,
            beta REAL,
            lastDividend REAL,
            range TEXT,
            change REAL,
            changePercentage REAL,
            volume INTEGER,
            averageVolume INTEGER,
            companyName TEXT,
            currency TEXT,
            cik TEXT,
            isin TEXT,
            cusip TEXT,
            exchangeFullName TEXT,
            exchange TEXT,
            industry TEXT,
            website TEXT,
            description TEXT,
            ceo TEXT,
            sector TEXT,
            country TEXT,
            fullTimeEmployees INTEGER,
            phone TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            image TEXT,
            ipoDate TEXT,
            defaultImage BOOLEAN,
            isEtf BOOLEAN,
            isActivelyTrading BOOLEAN,
            isAdr BOOLEAN,
            isFund BOOLEAN,
            last_updated_us INTEGER
        );
        """
    )


def is_fresh(*, table: str, symbol: str, max_last_updated_us: int) -> bool:
    db = stockdice.config.DB
    cursor = db.execute(
        f"SELECT last_updated_us FROM {table} WHERE symbol = :symbol", {"symbol": symbol}
    )
    previous_last_updated = cursor.fetchone()
    return (
        previous_last_updated is not None
        and previous_last_updated[0] is not None
        and previous_last_updated[0] > max_last_updated_us
    )
