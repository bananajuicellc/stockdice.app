from __future__ import annotations

import datetime
import logging

import stockdice.ratelimits
import stockdice.timeutils
import stockdice.stocklist

# https://site.financialmodelingprep.com/developer/docs/stable/income-statement
FMP_INCOME = "https://financialmodelingprep.com/stable/income-statement?symbol={symbol}&apikey={apikey}"



@stockdice.ratelimits.retry_fmp
async def download_income(*, session, symbol: str, max_age: datetime.timedelta):
    db = stockdice.config.DB
    now_us = stockdice.timeutils.now_in_microseconds()
    last_updated = db.execute("SELECT last_updated_us FROM income WHERE symbol = :symbol", {"symbol": symbol}).fetchone()
    if last_updated and datetime.timedelta(microseconds=now_us - last_updated[0]) <= max_age:
        logging.info(f"Data already fresh, skipping company_profile for {symbol}.")
        return

    url = FMP_INCOME.format(symbol=symbol, apikey=stockdice.config.FMP_API_KEY)
    async with session.get(url) as resp:
        resp_json = await stockdice.ratelimits.check_status(resp)

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
            ON CONFLICT (symbol, date) DO UPDATE SET
                reportedCurrency = EXCLUDED.reportedCurrency,
                cik = EXCLUDED.cik,
                filingDate = EXCLUDED.filingDate,
                acceptedDate = EXCLUDED.acceptedDate,
                fiscalYear = EXCLUDED.fiscalYear,
                period = EXCLUDED.period,
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


async def download_all(*, max_age: datetime.timedelta, session):
    for symbol in stockdice.stocklist.list_symbols():
        # TODO: if we get higher rate limits, maybe do this in parallel (or just use the bulk apis at that point).
        await download_income(max_age=max_age, session=session, symbol=symbol)
