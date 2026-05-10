INSERT INTO forex (symbol, from_currency, to_currency, price) VALUES ('USDUSD', 'USD', 'USD', 1.0) ON CONFLICT DO NOTHING;
