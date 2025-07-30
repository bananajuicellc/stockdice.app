from __future__ import annotations

import datetime
import logging

import stockdice.ratelimits
import stockdice.timeutils
import stockdice.stocklist

# https://site.financialmodelingprep.com/developer/docs/stable/profile-symbol 
FMP_COMPANY_PROFILE = "https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={apikey}"



@stockdice.ratelimits.retry_fmp
async def download_income(*, session, symbol: str, max_age: datetime.timedelta):
    db = stockdice.config.DB
    now_us = stockdice.timeutils.now_in_microseconds()
    last_updated = db.execute("SELECT last_updated_us FROM company_profile WHERE symbol = :symbol", {"symbol": symbol}).fetchone()
    if last_updated and datetime.timedelta(microseconds=now_us - last_updated[0]) <= max_age:
        logging.info(f"Data already fresh, skipping company_profile for {symbol}.")
        return

    url = FMP_COMPANY_PROFILE.format(symbol=symbol, apikey=stockdice.config.FMP_API_KEY)
    async with session.get(url) as resp:
        resp_json = await stockdice.ratelimits.check_status(resp)

        db.executemany(
            f"""
            INSERT INTO company_profile (
                symbol,
                price,
                marketCap,
                beta,
                lastDividend,
                range,
                change,
                changePercentage,
                volume,
                averageVolume,
                companyName,
                currency,
                cik,
                isin,
                cusip,
                exchangeFullName,
                exchange,
                industry,
                website,
                description,
                ceo,
                sector,
                country,
                fullTimeEmployees,
                phone,
                address,
                city,
                state,
                zip,
                image,
                ipoDate,
                defaultImage,
                isEtf,
                isActivelyTrading,
                isAdr,
                isFund,
                last_updated_us
            ) VALUES (
                :symbol,
                :price,
                :marketCap,
                :beta,
                :lastDividend,
                :range,
                :change,
                :changePercentage,
                :volume,
                :averageVolume,
                :companyName,
                :currency,
                :cik,
                :isin,
                :cusip,
                :exchangeFullName,
                :exchange,
                :industry,
                :website,
                :description,
                :ceo,
                :sector,
                :country,
                :fullTimeEmployees,
                :phone,
                :address,
                :city,
                :state,
                :zip,
                :image,
                :ipoDate,
                :defaultImage,
                :isEtf,
                :isActivelyTrading,
                :isAdr,
                :isFund,
                {now_us}
            )
            ON CONFLICT(symbol) DO UPDATE SET
                price = :price,
                marketCap = :marketCap,
                beta = :beta,
                lastDividend = :lastDividend,
                range = :range,
                change = :change,
                changePercentage = :changePercentage,
                volume = :volume,
                averageVolume = :averageVolume,
                companyName = :companyName,
                currency = :currency,
                cik = :cik,
                isin = :isin,
                cusip = :cusip,
                exchangeFullName = :exchangeFullName,
                exchange = :exchange,
                industry = :industry,
                website = :website,
                description = :description,
                ceo = :ceo,
                sector = :sector,
                country = :country,
                fullTimeEmployees = :fullTimeEmployees,
                phone = :phone,
                address = :address,
                city = :city,
                state = :state,
                zip = :zip,
                image = :image,
                ipoDate = :ipoDate,
                defaultImage = :defaultImage,
                isEtf = :isEtf,
                isActivelyTrading = :isActivelyTrading,
                isAdr = :isAdr,
                isFund = :isFund,
                last_updated_us = {now_us};
            """,
            resp_json,
        )
        db.commit()


async def download_all(*, max_age: datetime.timedelta, session):
    for symbol in stockdice.stocklist.list_symbols():
        # TODO: if we get higher rate limits, maybe do this in parallel (or just use the bulk apis at that point).
        await download_company_profile(max_age=max_age, session=session, symbol=symbol)
