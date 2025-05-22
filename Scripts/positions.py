# %%

import asyncio
from decimal import Decimal

import pandas as pd
from session_login import session_login
from shared_tasty_utils import fetch_market_data_efficient, get_option_chain_async
from tastytrade import Account, metrics
from tastytrade.market_data import get_market_data_by_type


# Define async helper functions
async def fetch_option_data(session, non_equity_symbols):
    """Fetches option chain data for a list of underlying symbols."""
    symbol_to_streamer = {}
    for ticker in non_equity_symbols:
        ticker_chain = await get_option_chain_async(session, ticker)
        for options_list in ticker_chain.values():
            for option in options_list:
                symbol_to_streamer[option.symbol] = option.streamer_symbol
    return symbol_to_streamer


async def fetch_greek_data(session, streamer_symbols):
    """Fetches market data, including greeks, for a list of option streamer symbols."""
    option_data = await fetch_market_data_efficient(session, streamer_symbols)
    rows = []
    for symbol, data in option_data.items():
        greeks = data.get("greeks")
        row = {"streamer_symbol": symbol}
        if greeks:
            row.update(
                {
                    "delta": Decimal(str(greeks.delta)),
                    "gamma": Decimal(str(greeks.gamma)),
                    "theta": Decimal(str(greeks.theta)),
                    "rho": Decimal(str(greeks.rho)),
                    "vega": Decimal(str(greeks.vega)),
                    "price": Decimal(str(greeks.price)),
                    "volatility": Decimal(str(greeks.volatility)),
                },
            )
        rows.append(row)
    return pd.DataFrame(rows, dtype=object)


def build_instrument_args(df):
    """Categorizes symbols from a DataFrame into instrument types for market data fetching."""
    args = {"indices": [], "cryptocurrencies": [], "equities": [], "futures": [], "future_options": [], "options": []}
    for _, row in df.iterrows():
        symbol = row["underlying_symbol"]
        instrument_type = row["instrument_type"]
        if instrument_type == "Equity":
            args["equities"].append(symbol)
        elif instrument_type == "Equity Option":
            args["options"].append(symbol)
        elif instrument_type == "Index":
            args["indices"].append(symbol)
        elif instrument_type == "Cryptocurrency":
            args["cryptocurrencies"].append(symbol)
        elif instrument_type == "Future":
            args["futures"].append(symbol)
        elif instrument_type == "Future Option":
            args["future_options"].append(symbol)
    for k in args:
        args[k] = list(set(args[k]))  # Ensure unique symbols
    return args


def flatten_market_metric_info(obj):
    """Flattens a market metric object from the Tastytrade API."""
    d = obj.__dict__.copy()
    if "earnings" in d and d["earnings"] is not None:
        d.update({f"earnings_{k}": v for k, v in d["earnings"].__dict__.items()})
        del d["earnings"]
    if "option_expiration_implied_volatilities" in d:
        for i, exp in enumerate(d["option_expiration_implied_volatilities"][:3]):
            for k, v in exp.__dict__.items():
                d[f"option_exp_{i + 1}_{k}"] = v
        del d["option_expiration_implied_volatilities"]
    return d


async def main():
    """Main asynchronous function to fetch and process trading account data"""
    session = session_login()

    # Fetch VIX and SPY prices for global calculations
    vix = Decimal(str(get_market_data_by_type(session, indices=["VIX"])[0].mark))
    # Define VIX ranges and corresponding multipliers for BPR calculation.
    # Using Decimal for bounds ensures consistent comparison with 'vix' (which is Decimal).
    vix_ranges = [
        (Decimal("0"), Decimal("15"), Decimal("0.25")),
        (Decimal("15"), Decimal("20"), Decimal("0.30")),
        (Decimal("20"), Decimal("30"), Decimal("0.35")),
        (Decimal("30"), Decimal("40"), Decimal("0.40")),
        (Decimal("40"), Decimal(str(float("inf"))), Decimal("0.50")),
    ]
    max_bpr = next(vix * multiplier for lower, upper, multiplier in vix_ranges if lower < vix <= upper)
    spy_price = Decimal(str(get_market_data_by_type(session, indices=["SPY"])[0].mark))

    accounts = Account.get(session)
    all_accounts_data = []  # To store data for each account

    for account in accounts:
        balance = account.get_balances(session)
        account_nickname = account.nickname
        if balance.net_liquidating_value > 0:
            # Account-specific calculations
            net_liquidating_value = Decimal(str(balance.net_liquidating_value))
            margin_requirements = account.get_margin_requirements(session)
            net_liquidating_used = Decimal(str(abs(margin_requirements.margin_requirement)))
            net_liquidating_percent = net_liquidating_used / net_liquidating_value
            bpr_usage = net_liquidating_percent > max_bpr
            bpr_left_to_use = (max_bpr - net_liquidating_percent) * net_liquidating_value
            net_liquidating_left = Decimal(str(margin_requirements.option_buying_power))
            max_delta = net_liquidating_value / spy_price
            max_theta = net_liquidating_value * Decimal("0.003")

            position_margin_requirements = [
                {
                    "underlying_symbol": entry.underlying_symbol,
                    "margin_requirement": Decimal(str(abs(entry.margin_requirement))),
                }
                for entry in margin_requirements.groups
            ]

            positions = account.get_positions(session)
            # Create DataFrame for positions
            positions_df = pd.DataFrame(
                [
                    {
                        "underlying_symbol": p.underlying_symbol,
                        "symbol": p.symbol,
                        "instrument_type": p.instrument_type.value if hasattr(p.instrument_type, "value") else str(p.instrument_type),
                        "quantity": Decimal(str(p.quantity)) * (-1 if p.quantity_direction == "Short" else 1),
                        "average_open_price": Decimal(str(p.average_open_price)),
                        "multiplier": Decimal(str(p.multiplier)),
                        "expires_at": p.expires_at,
                    }
                    for p in positions
                ],
                dtype=object,
            )

            market_data_args = build_instrument_args(positions_df)
            # Fetch market data for all instruments in positions
            data = get_market_data_by_type(
                session,
                indices=market_data_args["indices"],
                cryptocurrencies=market_data_args["cryptocurrencies"],
                equities=market_data_args["equities"],
                futures=market_data_args["futures"],
                future_options=market_data_args["future_options"],
                options=market_data_args["options"],
            )
            # Create DataFrame for market data
            market_df = pd.DataFrame(
                [
                    {
                        "symbol": d.symbol,
                        "mid": Decimal(str(d.mid)),
                        "prev_close": Decimal(str(d.prev_close)),
                        "daily_change": Decimal(str(Decimal(str(d.prev_close)) - Decimal(str(d.mid)))),
                    }
                    for d in data
                ],
                dtype=object,
            )

            merged_df = positions_df.merge(market_df, on="symbol", how="left")

            # Fetch option data (streamer symbols) for non-equity positions
            non_equity_symbols = merged_df[merged_df["instrument_type"] != "Equity"]["underlying_symbol"].unique().tolist()
            if non_equity_symbols:
                symbol_to_streamer = await fetch_option_data(session, non_equity_symbols)
                merged_df["streamer_symbol"] = merged_df["symbol"].map(symbol_to_streamer)
            else:
                merged_df["streamer_symbol"] = pd.NA

            # Fetch greek data for options
            streamer_symbols = merged_df["streamer_symbol"].dropna().unique().tolist()
            if streamer_symbols:
                greek_df = await fetch_greek_data(session, streamer_symbols)
                result_df = merged_df.merge(greek_df, on="streamer_symbol", how="left")
            else:
                result_df = merged_df.copy()
                # Ensure greek columns exist even if no options, fill with NA
                for col in ["delta", "gamma", "theta", "rho", "vega", "price", "volatility"]:
                    result_df[col] = pd.NA

            # Equities have a delta of 1.0 (or -1.0 for short, handled by quantity)
            result_df["delta"] = result_df["delta"].fillna(Decimal("1.0"))

            # Fetch market metrics for underlying symbols
            metrics_list = metrics.get_market_metrics(session, result_df.underlying_symbol.unique().tolist())
            flat_metrics = [flatten_market_metric_info(m) for m in metrics_list]
            metric_df = pd.DataFrame(flat_metrics)
            # Select and rename relevant metric columns
            cols = [
                "symbol",
                "implied_volatility_index_rank",
                "implied_volatility_percentile",
                "beta",
                "corr_spy_3month",
                "dividend_ex_date",
                "dividend_pay_date",
            ]
            # Check for additional columns and add them if they exist
            if "earnings_expected_report_date" in metric_df.columns:
                cols.append("earnings_expected_report_date")
            if "earnings_time_of_day" in metric_df.columns:
                cols.append("earnings_time_of_day")
            metric_df = metric_df[cols]

            metric_df = metric_df.rename(columns={"symbol": "underlying_symbol"})

            # Process date columns: convert to datetime, filter out past dates
            date_cols = [col for col in ["dividend_ex_date", "dividend_pay_date", "earnings_expected_report_date"] if col in metric_df.columns]
            for col in date_cols:
                metric_df[col] = pd.to_datetime(metric_df[col], errors="coerce")
            today = pd.Timestamp.today().normalize()
            for col in date_cols:
                metric_df[col] = metric_df[col].where(metric_df[col].isna() | (metric_df[col] >= today))
            # Convert valid dates back to object type if needed for display or specific serializers, or keep as datetime
            metric_df[date_cols] = metric_df[date_cols].where(metric_df[date_cols].isna(), metric_df[date_cols].astype(object))

            result_df = result_df.merge(metric_df, on="underlying_symbol", how="left")

            # Clean up and calculate position-level metrics
            result_df = result_df.drop(columns=["mid", "prev_close", "daily_change"], errors="ignore")
            result_df["open_price"] = result_df["average_open_price"] * result_df["multiplier"]
            result_df["current_price"] = result_df["price"] * result_df["multiplier"]
            result_df = result_df.drop(columns=["multiplier", "streamer_symbol", "volatility", "gamma", "rho", "vega"], errors="ignore")

            # Calculate total delta, theta, open price, current price, and profit
            result_df["delta"] = result_df["delta"] * result_df["quantity"]
            result_df["theta"] = result_df["theta"] * result_df["quantity"]
            result_df["open_price"] = result_df["open_price"] * result_df["quantity"] * Decimal("-1")
            result_df["current_price"] = result_df["current_price"] * result_df["quantity"] * Decimal("-1")
            result_df["profit"] = result_df["open_price"] - result_df["current_price"]

            # Dynamic aggregation to handle missing columns
            agg_dict = {
                "instrument_type": "first",
                "average_open_price": "sum",
                "expires_at": "first",
                "delta": "sum",
                "theta": "sum",
                "price": "first",
                "open_price": "sum",
                "current_price": "sum",
                "profit": "sum",
                "implied_volatility_index_rank": "first",
                "implied_volatility_percentile": "first",
                "beta": "first",
                "dividend_ex_date": "first",
                "dividend_pay_date": "first",
                "corr_spy_3month": "first",
            }
            # Add earnings columns only if they exist in result_df
            if "earnings_expected_report_date" in result_df.columns:
                agg_dict["earnings_expected_report_date"] = "first"
            if "earnings_time_of_day" in result_df.columns:
                agg_dict["earnings_time_of_day"] = "first"

            # Group by underlying symbol and aggregate
            result_df = result_df.groupby("underlying_symbol").agg(agg_dict).reset_index()
            result_df["percent_profit"] = (result_df["profit"] / result_df["open_price"]) * Decimal("100")

            market_underlying_args = build_instrument_args(result_df)
            market_data = get_market_data_by_type(
                session,
                indices=market_underlying_args["indices"],
                cryptocurrencies=market_underlying_args["cryptocurrencies"],
                equities=market_underlying_args["equities"],
                futures=market_underlying_args["futures"],
                future_options=market_underlying_args["future_options"],
                options=market_underlying_args["options"],
            )
            # Create DataFrame for underlying market data
            market_df = pd.DataFrame(
                [
                    {
                        "underlying_symbol": d.symbol,
                        "mid": Decimal(str(d.mid)),
                        "daily_change": Decimal(str(d.prev_close - d.mid)),
                    }
                    for d in market_data
                ],
                dtype=object,
            )
            # Calculate daily percentage change for underlyings
            market_df["prev_close"] = market_df["mid"] - market_df["daily_change"]
            market_df["pct_change"] = market_df["daily_change"] / market_df["prev_close"] * Decimal("-100")

            result_df = result_df.merge(market_df, on="underlying_symbol", how="left")
            result_df["beta_weighted_delta"] = result_df["delta"] * result_df["beta"] * (result_df["mid"] / spy_price)

            # Store the data for this account
            current_account_data = {"account_object": account, "account_nickname": account_nickname, "result_df": result_df}
            all_accounts_data.append(current_account_data)

    return all_accounts_data


### Uncomment to test
if __name__ == "__main__":
    accounts_processed_data = asyncio.run(main())
    for data in accounts_processed_data:
        print(f"\n--- Account: {data['account_nickname']} ({data['account_object'].account_number}) ---")
        print("Processed DataFrame:")
        print(data["result_df"])
        print("-" * 50)
