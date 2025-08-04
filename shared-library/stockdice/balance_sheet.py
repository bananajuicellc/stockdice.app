from __future__ import annotations

import asyncio
import datetime
import logging

import httpx

import stockdice.company_profile
import stockdice.ratelimits
import stockdice.timeutils
import stockdice.stocklist

# https://site.financialmodelingprep.com/developer/docs/stable/balance-sheet-statement
# https://www.investopedia.com/terms/b/balancesheet.asp
FMP_BALANCE_SHEET = "https://financialmodelingprep.com/stable/balance-sheet-statement?symbol={symbol}&apikey={apikey}"


@stockdice.ratelimits.retry_fmp
async def download_balance_sheet(
    *, client: httpx.AsyncClient, symbol: str, max_age: datetime.timedelta
):
    db = stockdice.config.DB
    now_us = stockdice.timeutils.now_in_microseconds()
    last_updated = db.execute(
        """
        SELECT last_updated_us
        FROM balance_sheet
        WHERE symbol = :symbol
        ORDER BY last_updated_us DESC
        LIMIT 1;
        """,
        {"symbol": symbol},
    ).fetchone()
    if (
        last_updated
        and datetime.timedelta(microseconds=now_us - last_updated[0]) <= max_age
    ):
        logging.debug(f"Data already fresh, skipping balance_sheet for {symbol}.")
        return

    if stockdice.company_profile.is_fund_or_etf(symbol):
        logging.debug(f"{symbol} is a fund or ETF, skipping.")
        db.execute(
            f"""
            INSERT INTO balance_sheet (
                "symbol", "fiscalYear", "period", "last_updated_us"
            ) VALUES (
                :symbol, :fiscalYear, :period, :last_updated_us
            ) ON CONFLICT (symbol, fiscalYear, period) DO UPDATE SET
                "last_updated_us" = excluded."last_updated_us";
            """,
            {
                "symbol": symbol,
                "fiscalYear": None,
                "period": None,
                "last_updated_us": now_us,
            },
        )
        db.commit()
        return

    url = FMP_BALANCE_SHEET.format(symbol=symbol, apikey=stockdice.config.FMP_API_KEY)
    resp = await stockdice.ratelimits.get(client, url)
    resp_json = stockdice.ratelimits.check_status(resp)

    # Empty, but successfull response means we don't have the data available, so
    # let's skip it for now.
    if not resp_json:
        logging.info(f"No balance_sheet data available for {symbol}.")
        db.execute(
            f"""
            INSERT INTO balance_sheet (
                "symbol", "fiscalYear", "period", "last_updated_us"
            ) VALUES (
                :symbol, :fiscalYear, :period, :last_updated_us
            ) ON CONFLICT (symbol, fiscalYear, period) DO UPDATE SET
                "last_updated_us" = excluded."last_updated_us";
            """,
            {
                "symbol": symbol,
                "fiscalYear": None,
                "period": None,
                "last_updated_us": now_us,
            },
        )
        db.commit()
        return

    db.executemany(
        f"""
        INSERT INTO balance_sheet (
            "date", "symbol", "reportedCurrency", "cik", "filingDate", "acceptedDate", "fiscalYear", "period",
            "cashAndCashEquivalents", "shortTermInvestments", "cashAndShortTermInvestments", "netReceivables",
            "accountsReceivables", "otherReceivables", "inventory", "prepaids", "otherCurrentAssets",
            "totalCurrentAssets", "propertyPlantEquipmentNet", "goodwill", "intangibleAssets",
            "goodwillAndIntangibleAssets", "longTermInvestments", "taxAssets", "otherNonCurrentAssets",
            "totalNonCurrentAssets", "otherAssets", "totalAssets", "totalPayables", "accountPayables",
            "otherPayables", "accruedExpenses", "shortTermDebt", "capitalLeaseObligationsCurrent", "taxPayables",
            "deferredRevenue", "otherCurrentLiabilities", "totalCurrentLiabilities", "longTermDebt",
            "deferredRevenueNonCurrent", "deferredTaxLiabilitiesNonCurrent", "otherNonCurrentLiabilities",
            "totalNonCurrentLiabilities", "otherLiabilities", "capitalLeaseObligations", "totalLiabilities",
            "treasuryStock", "preferredStock", "commonStock", "retainedEarnings", "additionalPaidInCapital",
            "accumulatedOtherComprehensiveIncomeLoss", "otherTotalStockholdersEquity", "totalStockholdersEquity",
            "totalEquity", "minorityInterest", "totalLiabilitiesAndTotalEquity", "totalInvestments",
            "totalDebt", "netDebt", "last_updated_us"
        ) VALUES (
            :date, :symbol, :reportedCurrency, :cik, :filingDate, :acceptedDate, :fiscalYear, :period,
            :cashAndCashEquivalents, :shortTermInvestments, :cashAndShortTermInvestments, :netReceivables,
            :accountsReceivables, :otherReceivables, :inventory, :prepaids, :otherCurrentAssets,
            :totalCurrentAssets, :propertyPlantEquipmentNet, :goodwill, :intangibleAssets,
            :goodwillAndIntangibleAssets, :longTermInvestments, :taxAssets, :otherNonCurrentAssets,
            :totalNonCurrentAssets, :otherAssets, :totalAssets, :totalPayables, :accountPayables,
            :otherPayables, :accruedExpenses, :shortTermDebt, :capitalLeaseObligationsCurrent, :taxPayables,
            :deferredRevenue, :otherCurrentLiabilities, :totalCurrentLiabilities, :longTermDebt,
            :deferredRevenueNonCurrent, :deferredTaxLiabilitiesNonCurrent, :otherNonCurrentLiabilities,
            :totalNonCurrentLiabilities, :otherLiabilities, :capitalLeaseObligations, :totalLiabilities,
            :treasuryStock, :preferredStock, :commonStock, :retainedEarnings, :additionalPaidInCapital,
            :accumulatedOtherComprehensiveIncomeLoss, :otherTotalStockholdersEquity, :totalStockholdersEquity,
            :totalEquity, :minorityInterest, :totalLiabilitiesAndTotalEquity, :totalInvestments,
            :totalDebt, :netDebt, {now_us}
        )
        ON CONFLICT(symbol, fiscalYear, period) DO UPDATE SET
            "date" = excluded."date",
            "reportedCurrency" = excluded."reportedCurrency",
            "cik" = excluded."cik",
            "filingDate" = excluded."filingDate",
            "acceptedDate" = excluded."acceptedDate",
            "cashAndCashEquivalents" = excluded."cashAndCashEquivalents",
            "shortTermInvestments" = excluded."shortTermInvestments",
            "cashAndShortTermInvestments" = excluded."cashAndShortTermInvestments",
            "netReceivables" = excluded."netReceivables",
            "accountsReceivables" = excluded."accountsReceivables",
            "otherReceivables" = excluded."otherReceivables",
            "inventory" = excluded."inventory",
            "prepaids" = excluded."prepaids",
            "otherCurrentAssets" = excluded."otherCurrentAssets",
            "totalCurrentAssets" = excluded."totalCurrentAssets",
            "propertyPlantEquipmentNet" = excluded."propertyPlantEquipmentNet",
            "goodwill" = excluded."goodwill",
            "intangibleAssets" = excluded."intangibleAssets",
            "goodwillAndIntangibleAssets" = excluded."goodwillAndIntangibleAssets",
            "longTermInvestments" = excluded."longTermInvestments",
            "taxAssets" = excluded."taxAssets",
            "otherNonCurrentAssets" = excluded."otherNonCurrentAssets",
            "totalNonCurrentAssets" = excluded."totalNonCurrentAssets",
            "otherAssets" = excluded."otherAssets",
            "totalAssets" = excluded."totalAssets",
            "totalPayables" = excluded."totalPayables",
            "accountPayables" = excluded."accountPayables",
            "otherPayables" = excluded."otherPayables",
            "accruedExpenses" = excluded."accruedExpenses",
            "shortTermDebt" = excluded."shortTermDebt",
            "capitalLeaseObligationsCurrent" = excluded."capitalLeaseObligationsCurrent",
            "taxPayables" = excluded."taxPayables",
            "deferredRevenue" = excluded."deferredRevenue",
            "otherCurrentLiabilities" = excluded."otherCurrentLiabilities",
            "totalCurrentLiabilities" = excluded."totalCurrentLiabilities",
            "longTermDebt" = excluded."longTermDebt",
            "deferredRevenueNonCurrent" = excluded."deferredRevenueNonCurrent",
            "deferredTaxLiabilitiesNonCurrent" = excluded."deferredTaxLiabilitiesNonCurrent",
            "otherNonCurrentLiabilities" = excluded."otherNonCurrentLiabilities",
            "totalNonCurrentLiabilities" = excluded."totalNonCurrentLiabilities",
            "otherLiabilities" = excluded."otherLiabilities",
            "capitalLeaseObligations" = excluded."capitalLeaseObligations",
            "totalLiabilities" = excluded."totalLiabilities",
            "treasuryStock" = excluded."treasuryStock",
            "preferredStock" = excluded."preferredStock",
            "commonStock" = excluded."commonStock",
            "retainedEarnings" = excluded."retainedEarnings",
            "additionalPaidInCapital" = excluded."additionalPaidInCapital",
            "accumulatedOtherComprehensiveIncomeLoss" = excluded."accumulatedOtherComprehensiveIncomeLoss",
            "otherTotalStockholdersEquity" = excluded."otherTotalStockholdersEquity",
            "totalStockholdersEquity" = excluded."totalStockholdersEquity",
            "totalEquity" = excluded."totalEquity",
            "minorityInterest" = excluded."minorityInterest",
            "totalLiabilitiesAndTotalEquity" = excluded."totalLiabilitiesAndTotalEquity",
            "totalInvestments" = excluded."totalInvestments",
            "totalDebt" = excluded."totalDebt",
            "netDebt" = excluded."netDebt",
            "last_updated_us" = excluded."last_updated_us";
        """,
        resp_json,
    )
    db.commit()


async def download_all(*, max_age: datetime.timedelta, client: httpx.AsyncClient):
    return await asyncio.gather(
        *[
            download_balance_sheet(max_age=max_age, client=client, symbol=symbol)
            for symbol in stockdice.stocklist.list_symbols()
        ]
    )
