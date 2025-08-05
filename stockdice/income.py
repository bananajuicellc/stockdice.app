from __future__ import annotations

import asyncio
import datetime
import logging

import httpx

import stockdice.company_profile
import stockdice.ratelimits
import stockdice.timeutils
import stockdice.stocklist

# https://site.financialmodelingprep.com/developer/docs/stable/income-statement
FMP_INCOME = "https://financialmodelingprep.com/stable/income-statement?symbol={symbol}&apikey={apikey}"


@stockdice.ratelimits.retry_fmp
async def download_income(
    *, client: httpx.AsyncClient, symbol: str, max_age: datetime.timedelta
):
    db = stockdice.config.DB
    now_us = stockdice.timeutils.now_in_microseconds()
    last_updated = db.execute(
        """
        SELECT last_updated_us
        FROM income
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
        logging.debug(f"Data already fresh, skipping income for {symbol}.")
        return

    if stockdice.company_profile.is_fund_or_etf(symbol):
        logging.debug(f"{symbol} is a fund or ETF, skipping.")
        db.execute(
            f"""
            INSERT INTO income (
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

    url = FMP_INCOME.format(symbol=symbol, apikey=stockdice.config.FMP_API_KEY)
    resp = await stockdice.ratelimits.get(client, url)
    resp_json = stockdice.ratelimits.check_status(resp)

    # Empty, but successfull response means we don't have the data available, so
    # let's skip it for now.
    if not resp_json:
        logging.info(f"No income data available for {symbol}.")
        db.execute(
            f"""
            INSERT INTO income (
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
        INSERT INTO income (
            date,
            symbol,
            reportedCurrency,
            cik,
            filingDate,
            acceptedDate,
            fiscalYear,
            period,
            revenue,
            costOfRevenue,
            grossProfit,
            researchAndDevelopmentExpenses,
            generalAndAdministrativeExpenses,
            sellingAndMarketingExpenses,
            sellingGeneralAndAdministrativeExpenses,
            otherExpenses,
            operatingExpenses,
            costAndExpenses,
            netInterestIncome,
            interestIncome,
            interestExpense,
            depreciationAndAmortization,
            ebitda,
            ebit,
            nonOperatingIncomeExcludingInterest,
            operatingIncome,
            totalOtherIncomeExpensesNet,
            incomeBeforeTax,
            incomeTaxExpense,
            netIncomeFromContinuingOperations,
            netIncomeFromDiscontinuedOperations,
            otherAdjustmentsToNetIncome,
            netIncome,
            netIncomeDeductions,
            bottomLineNetIncome,
            eps,
            epsDiluted,
            weightedAverageShsOut,
            weightedAverageShsOutDil,
            last_updated_us
        ) VALUES (
            :date,
            :symbol,
            :reportedCurrency,
            :cik,
            :filingDate,
            :acceptedDate,
            :fiscalYear,
            :period,
            :revenue,
            :costOfRevenue,
            :grossProfit,
            :researchAndDevelopmentExpenses,
            :generalAndAdministrativeExpenses,
            :sellingAndMarketingExpenses,
            :sellingGeneralAndAdministrativeExpenses,
            :otherExpenses,
            :operatingExpenses,
            :costAndExpenses,
            :netInterestIncome,
            :interestIncome,
            :interestExpense,
            :depreciationAndAmortization,
            :ebitda,
            :ebit,
            :nonOperatingIncomeExcludingInterest,
            :operatingIncome,
            :totalOtherIncomeExpensesNet,
            :incomeBeforeTax,
            :incomeTaxExpense,
            :netIncomeFromContinuingOperations,
            :netIncomeFromDiscontinuedOperations,
            :otherAdjustmentsToNetIncome,
            :netIncome,
            :netIncomeDeductions,
            :bottomLineNetIncome,
            :eps,
            :epsDiluted,
            :weightedAverageShsOut,
            :weightedAverageShsOutDil,
            {now_us}
        )
        ON CONFLICT (symbol, fiscalYear, period) DO UPDATE SET
            reportedCurrency = EXCLUDED.reportedCurrency,
            cik = EXCLUDED.cik,
            date = EXCLUDED.date,
            filingDate = EXCLUDED.filingDate,
            acceptedDate = EXCLUDED.acceptedDate,
            revenue = EXCLUDED.revenue,
            costOfRevenue = EXCLUDED.costOfRevenue,
            grossProfit = EXCLUDED.grossProfit,
            researchAndDevelopmentExpenses = EXCLUDED.researchAndDevelopmentExpenses,
            generalAndAdministrativeExpenses = EXCLUDED.generalAndAdministrativeExpenses,
            sellingAndMarketingExpenses = EXCLUDED.sellingAndMarketingExpenses,
            sellingGeneralAndAdministrativeExpenses = EXCLUDED.sellingGeneralAndAdministrativeExpenses,
            otherExpenses = EXCLUDED.otherExpenses,
            operatingExpenses = EXCLUDED.operatingExpenses,
            costAndExpenses = EXCLUDED.costAndExpenses,
            netInterestIncome = EXCLUDED.netInterestIncome,
            interestIncome = EXCLUDED.interestIncome,
            interestExpense = EXCLUDED.interestExpense,
            depreciationAndAmortization = EXCLUDED.depreciationAndAmortization,
            ebitda = EXCLUDED.ebitda,
            ebit = EXCLUDED.ebit,
            nonOperatingIncomeExcludingInterest = EXCLUDED.nonOperatingIncomeExcludingInterest,
            operatingIncome = EXCLUDED.operatingIncome,
            totalOtherIncomeExpensesNet = EXCLUDED.totalOtherIncomeExpensesNet,
            incomeBeforeTax = EXCLUDED.incomeBeforeTax,
            incomeTaxExpense = EXCLUDED.incomeTaxExpense,
            netIncomeFromContinuingOperations = EXCLUDED.netIncomeFromContinuingOperations,
            netIncomeFromDiscontinuedOperations = EXCLUDED.netIncomeFromDiscontinuedOperations,
            otherAdjustmentsToNetIncome = EXCLUDED.otherAdjustmentsToNetIncome,
            netIncome = EXCLUDED.netIncome,
            netIncomeDeductions = EXCLUDED.netIncomeDeductions,
            bottomLineNetIncome = EXCLUDED.bottomLineNetIncome,
            eps = EXCLUDED.eps,
            epsDiluted = EXCLUDED.epsDiluted,
            weightedAverageShsOut = EXCLUDED.weightedAverageShsOut,
            weightedAverageShsOutDil = EXCLUDED.weightedAverageShsOutDil,
            last_updated_us = {now_us};
        """,
        resp_json,
    )
    db.commit()


async def download_all(*, max_age: datetime.timedelta, client: httpx.AsyncClient):
    return await asyncio.gather(
        *[
            download_income(max_age=max_age, client=client, symbol=symbol)
            for symbol in stockdice.stocklist.list_symbols()
        ]
    )
