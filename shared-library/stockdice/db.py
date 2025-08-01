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

import logging

import stockdice.config


def _table_exists(db, table_name):
    # https://stackoverflow.com/a/1604121/101923
    exists = db.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
    ).fetchone()
    return exists is not None


def create_all_tables(db, *, reset: bool):
    create_balance_sheet(db, reset=reset)
    create_company_profile(db, reset=reset)
    create_forex(db, reset=reset)
    create_income(db, reset=reset)
    create_symbols(db, reset=reset)


def create_balance_sheet(db, *, reset: bool):
    if reset:
        db.execute("DROP TABLE IF EXISTS balance_sheet;")
    elif _table_exists(db, "balance_sheet"):
        logging.warning("balance_sheet already exists, skipping")
        return
    
    db.execute(
        """
        CREATE TABLE balance_sheet (
            "date" TEXT,
            "symbol" TEXT NOT NULL,
            "reportedCurrency" TEXT,
            "cik" TEXT NOT NULL,
            "filingDate" TEXT,
            "acceptedDate" TEXT,
            "fiscalYear" INTEGER NOT NULL,
            "period" TEXT NOT NULL,
            "cashAndCashEquivalents" INTEGER,
            "shortTermInvestments" INTEGER,
            "cashAndShortTermInvestments" INTEGER,
            "netReceivables" INTEGER,
            "accountsReceivables" INTEGER,
            "otherReceivables" INTEGER,
            "inventory" INTEGER,
            "prepaids" INTEGER,
            "otherCurrentAssets" INTEGER,
            "totalCurrentAssets" INTEGER,
            "propertyPlantEquipmentNet" INTEGER,
            "goodwill" INTEGER,
            "intangibleAssets" INTEGER,
            "goodwillAndIntangibleAssets" INTEGER,
            "longTermInvestments" INTEGER,
            "taxAssets" INTEGER,
            "otherNonCurrentAssets" INTEGER,
            "totalNonCurrentAssets" INTEGER,
            "otherAssets" INTEGER,
            "totalAssets" INTEGER,
            "totalPayables" INTEGER,
            "accountPayables" INTEGER,
            "otherPayables" INTEGER,
            "accruedExpenses" INTEGER,
            "shortTermDebt" INTEGER,
            "capitalLeaseObligationsCurrent" INTEGER,
            "taxPayables" INTEGER,
            "deferredRevenue" INTEGER,
            "otherCurrentLiabilities" INTEGER,
            "totalCurrentLiabilities" INTEGER,
            "longTermDebt" INTEGER,
            "deferredRevenueNonCurrent" INTEGER,
            "deferredTaxLiabilitiesNonCurrent" INTEGER,
            "otherNonCurrentLiabilities" INTEGER,
            "totalNonCurrentLiabilities" INTEGER,
            "otherLiabilities" INTEGER,
            "capitalLeaseObligations" INTEGER,
            "totalLiabilities" INTEGER,
            "treasuryStock" INTEGER,
            "preferredStock" INTEGER,
            "commonStock" INTEGER,
            "retainedEarnings" INTEGER,
            "additionalPaidInCapital" INTEGER,
            "accumulatedOtherComprehensiveIncomeLoss" INTEGER,
            "otherTotalStockholdersEquity" INTEGER,
            "totalStockholdersEquity" INTEGER,
            "totalEquity" INTEGER,
            "minorityInterest" INTEGER,
            "totalLiabilitiesAndTotalEquity" INTEGER,
            "totalInvestments" INTEGER,
            "totalDebt" INTEGER,
            "netDebt" INTEGER,
            last_updated_us INTEGER,
            PRIMARY KEY ("symbol", "fiscalYear", "period")
        );
        """
    )
    db.commit()



def create_forex(db, *, reset: bool):
    if reset:
        db.execute("DROP TABLE IF EXISTS forex;")
    elif _table_exists(db, "forex"):
        logging.warning("forex already exists, skipping")
        return

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


def create_income(db, *, reset: bool):
    if reset:
        db.execute("DROP TABLE IF EXISTS income;")
    elif _table_exists(db, "income"):
        logging.warning("income already exists, skipping")
        return

    db.execute(
        """
        CREATE TABLE income (
            date STRING,
            symbol STRING,
            reportedCurrency TEXT,
            cik TEXT,
            filingDate TEXT,
            acceptedDate TEXT,
            fiscalYear INTEGER,
            period TEXT,
            revenue INTEGER,
            costOfRevenue INTEGER,
            grossProfit INTEGER,
            researchAndDevelopmentExpenses INTEGER,
            generalAndAdministrativeExpenses INTEGER,
            sellingAndMarketingExpenses INTEGER,
            sellingGeneralAndAdministrativeExpenses INTEGER,
            otherExpenses INTEGER,
            operatingExpenses INTEGER,
            costAndExpenses INTEGER,
            netInterestIncome INTEGER,
            interestIncome INTEGER,
            interestExpense INTEGER,
            depreciationAndAmortization INTEGER,
            ebitda INTEGER,
            ebit INTEGER,
            nonOperatingIncomeExcludingInterest INTEGER,
            operatingIncome INTEGER,
            totalOtherIncomeExpensesNet INTEGER,
            incomeBeforeTax INTEGER,
            incomeTaxExpense INTEGER,
            netIncomeFromContinuingOperations INTEGER,
            netIncomeFromDiscontinuedOperations INTEGER,
            otherAdjustmentsToNetIncome INTEGER,
            netIncome INTEGER,
            netIncomeDeductions INTEGER,
            bottomLineNetIncome INTEGER,
            eps REAL,
            epsDiluted REAL,
            weightedAverageShsOut INTEGER,
            weightedAverageShsOutDil INTEGER,
            last_updated_us INTEGER,
            PRIMARY KEY (symbol, fiscalYear, period)
        );
        """
    )
    db.commit()


def create_symbols(db, *, reset: bool):
    if reset:
        db.execute("DROP TABLE IF EXISTS symbol;")
    elif _table_exists(db, "symbol"):
        logging.warning("symbol already exists, skipping")
        return

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


def create_company_profile(db, *, reset: bool):
    if reset:
        db.execute("DROP TABLE IF EXISTS company_profile;")
    elif _table_exists(db, "company_profile"):
        logging.warning("company_profile already exists, skipping")
        return

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
        f"SELECT last_updated_us FROM {table} WHERE symbol = :symbol",
        {"symbol": symbol},
    )
    previous_last_updated = cursor.fetchone()
    return (
        previous_last_updated is not None
        and previous_last_updated[0] is not None
        and previous_last_updated[0] > max_last_updated_us
    )
