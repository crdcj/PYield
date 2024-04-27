import pyield as yd


def test_settlement_rate_with_old_holiday_list():
    settlement_rates = {
        "DI1N27": 0.09809,
        "DI1F33": 0.10368,
    }

    # 22-12-2023 is before the new holiday calendar
    df = yd.fetch_asset(asset_code="DI1", reference_date="2023-12-22")
    tickers = list(settlement_rates.keys())  # noqa: F841
    result = df.query("TickerSymbol in @tickers")["SettlementRate"].to_list()
    assert result == list(settlement_rates.values())


def test_settlement_rates_with_current_holiday_list():
    settlement_rates = {
        "DI1F24": 0.11644,
        "DI1J24": 0.11300,
        "DI1N24": 0.10786,
        "DI1V24": 0.10321,
        "DI1F25": 0.10031,
        "DI1J25": 0.09852,
        "DI1N25": 0.09715,
        "DI1V25": 0.09651,
        "DI1F26": 0.09583,
        "DI1N26": 0.09631,
        "DI1F27": 0.09683,
        "DI1N27": 0.09794,
        "DI1F29": 0.10042,
        "DI1F31": 0.10240,
        "DI1F33": 0.10331,
    }
    df = yd.fetch_asset(asset_code="DI1", reference_date="2023-12-26")
    tickers = list(settlement_rates.keys())  # noqa: F841
    results = df.query("TickerSymbol in @tickers")["SettlementRate"].to_list()
    assert results == list(settlement_rates.values())
