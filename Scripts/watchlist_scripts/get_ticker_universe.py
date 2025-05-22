import logging
import os

import pandas as pd
from tastytrade import instruments, metrics
from tastytrade.watchlists import PrivateWatchlist

# Configure logging
# Logs to console by default. Change handler for file logging if needed.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

# Constants for filtering criteria
MIN_LIQUIDITY_NON_ETF = 2
MIN_LIQUIDITY_ETF = 3
MIN_EXPIRATION_DAYS = 5
MAX_EXPIRATION_DAYS = 83
MIN_EXPIRATIONS_COUNT = 5
WATCHLIST_NAME = "TickerUniverse"


def get_market_metrics(session, filtered_symbols, chunk_size=100):
    """Fetch market metrics for a list of symbols, skipping any bad symbols and logging errors.
    Also updates a persistent list of bad symbols to avoid re-querying them.
    """
    metrics_data = []
    known_bad_symbols = set()
    bad_symbols_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Data", "bad_symbols.txt"))

    # Load bad symbols if file exists
    if os.path.exists(bad_symbols_path):
        with open(bad_symbols_path) as f:
            known_bad_symbols = {line.strip() for line in f if line.strip()}
        logging.info(f"Loaded {len(known_bad_symbols)} known bad symbols from {bad_symbols_path}")

    # Filter out known bad symbols before making API calls
    symbols_to_query = [s for s in filtered_symbols if s not in known_bad_symbols]
    newly_bad_symbols = set()

    # Consider more advanced retry mechanisms for chunk failures,
    # e.g., binary search within the chunk or dynamic chunk size reduction,
    # if performance with individual retries becomes an issue.
    for i in range(0, len(symbols_to_query), chunk_size):
        chunk = symbols_to_query[i : i + chunk_size]
        try:
            chunk_metrics = metrics.get_market_metrics(session, chunk)
            for metric in chunk_metrics:
                metrics_data.append(
                    {
                        "symbol": metric.symbol,
                        "liquidity_rating": metric.liquidity_rating,
                        "option_expiration_implied_volatilities": metric.option_expiration_implied_volatilities,
                    },
                )
        except Exception as e_chunk:
            # Try each symbol individually if the chunk fails
            logging.warning(f"Failed to fetch metrics for chunk (size {len(chunk)}). Error: {e_chunk}. Retrying symbols individually.", exc_info=True)
            for symbol in chunk:
                try:
                    metric = metrics.get_market_metrics(session, [symbol])
                    for m in metric:
                        metrics_data.append(
                            {
                                "symbol": m.symbol,
                                "liquidity_rating": m.liquidity_rating,
                                "option_expiration_implied_volatilities": m.option_expiration_implied_volatilities,
                            },
                        )
                except Exception:
                    logging.error(f"Error processing symbol: {symbol}. Adding to bad symbols list.", exc_info=True)
                    newly_bad_symbols.add(symbol)

    # Save all bad symbols (known + newly found)
    if newly_bad_symbols:
        all_bad_symbols = known_bad_symbols.union(newly_bad_symbols)
        try:
            with open(bad_symbols_path, "w") as f:
                for sym in sorted(list(all_bad_symbols)):  # Ensure it's a list for sorting
                    f.write(f"{sym}\n")
            logging.info(
                f"Updated bad symbols file at {bad_symbols_path} with {len(newly_bad_symbols)} new bad symbols. Total: {len(all_bad_symbols)}.",
            )
        except OSError as e:
            logging.error(f"Could not write to bad symbols file {bad_symbols_path}: {e}", exc_info=True)

    return pd.DataFrame(metrics_data), list(newly_bad_symbols)


def _fetch_active_optionable_equities(session):
    """Fetches all active equities and filters for optionable ones."""
    logging.info("Fetching active optionable equities...")
    active_equities = instruments.Equity.get_active_equities(session)
    equities_df = pd.DataFrame([e.__dict__ for e in active_equities])
    # Filter for equities that are optionable (have option_tick_sizes)
    optionable_df = equities_df[equities_df["option_tick_sizes"].notna()][["symbol", "description", "is_etf"]]
    logging.info(f"Found {len(optionable_df)} active optionable equities.")
    return optionable_df


def _get_metrics_and_filter_by_liquidity(session, optionable_df):
    """Fetches market metrics and filters symbols by liquidity and type (ETF/stock)."""
    if optionable_df.empty:
        logging.warning("No optionable equities provided to fetch metrics for.")
        return pd.DataFrame(), []

    # Sort symbols alphabetically before fetching metrics
    initial_symbols = sorted(optionable_df["symbol"].unique().tolist())
    logging.info(f"Fetching market metrics for {len(initial_symbols)} symbols...")
    metrics_df, _ = get_market_metrics(session, initial_symbols)  # newly_bad_symbols is handled by get_market_metrics

    if metrics_df.empty:
        logging.warning("No market metrics were fetched. Cannot proceed with liquidity filtering.")
        return pd.DataFrame(), []
    logging.info(f"Successfully fetched market metrics for {len(metrics_df)} symbols.")

    # Merge metrics with ETF type information
    metrics_with_type = pd.merge(
        metrics_df,
        optionable_df[["symbol", "is_etf"]],
        on="symbol",
        how="left",
    )

    # Apply liquidity filters:
    # - Non-ETFs: liquidity_rating >= MIN_LIQUIDITY_NON_ETF
    # - ETFs: liquidity_rating >= MIN_LIQUIDITY_ETF
    filtered_metrics = metrics_with_type[
        ((metrics_with_type["is_etf"] == False) & (metrics_with_type["liquidity_rating"] >= MIN_LIQUIDITY_NON_ETF))
        | ((metrics_with_type["is_etf"] == True) & (metrics_with_type["liquidity_rating"] >= MIN_LIQUIDITY_ETF))
    ]

    valid_symbols_after_filter = filtered_metrics["symbol"].unique().tolist()
    logging.info(f"Number of symbols after liquidity/type filter: {len(valid_symbols_after_filter)}")
    return filtered_metrics, valid_symbols_after_filter


def _filter_by_option_expirations(filtered_metrics_df):
    """Filters symbols based on the number of option expirations within a target window."""
    if filtered_metrics_df.empty or "option_expiration_implied_volatilities" not in filtered_metrics_df.columns:
        logging.warning("Filtered metrics DataFrame is empty or missing necessary columns. Skipping expiration filter.")
        return []

    logging.info("Filtering symbols by option expiration dates...")
    # Explode the DataFrame to have one row per option expiration
    exploded = filtered_metrics_df.explode("option_expiration_implied_volatilities").reset_index(drop=True)

    # Extract expiration dates and convert to datetime objects, coercing errors for robust parsing
    exploded["expiration_date"] = pd.to_datetime(
        exploded["option_expiration_implied_volatilities"].apply(
            lambda opt: getattr(opt, "expiration_date", None) if opt else None,
        ),
        errors="coerce",
    )
    exploded.dropna(subset=["expiration_date"], inplace=True)  # Remove rows where date conversion failed

    today = pd.Timestamp.today().normalize()
    # Define the target window for expirations
    filtered_expirations = exploded[
        (exploded["expiration_date"] > today + pd.Timedelta(days=MIN_EXPIRATION_DAYS))
        & (exploded["expiration_date"] < today + pd.Timedelta(days=MAX_EXPIRATION_DAYS))
    ]

    # Count the number of valid expirations per symbol
    symbol_counts = filtered_expirations["symbol"].value_counts()

    # Filter for symbols with a minimum number of expirations
    sufficient_expirations_symbols = symbol_counts[symbol_counts >= MIN_EXPIRATIONS_COUNT].index.tolist()
    logging.info(f"Found {len(sufficient_expirations_symbols)} symbols with at least {MIN_EXPIRATIONS_COUNT} expirations in the target window.")
    return sufficient_expirations_symbols


def _upload_watchlist_to_tastytrade(session, symbols_list, watchlist_name):
    """Removes an existing watchlist (if any) and uploads a new one with the given symbols."""
    if not symbols_list:
        logging.warning(f"No symbols to upload for watchlist '{watchlist_name}'. Skipping upload.")
        return

    logging.info(f"Preparing to upload watchlist '{watchlist_name}' with {len(symbols_list)} tickers.")
    tickers_watchlist_entries = [{"symbol": symbol} for symbol in symbols_list]

    # Attempt to remove the existing watchlist to prevent duplicates or errors
    try:
        PrivateWatchlist.remove(session, watchlist_name)
        logging.info(f"Successfully removed existing watchlist: '{watchlist_name}'.")
    except Exception as e:
        # Log as info because it's common for the watchlist not to exist on the first run
        logging.info(f"Could not remove watchlist '{watchlist_name}' (it might not exist): {e}")

    # Create and upload the new watchlist
    watchlist = PrivateWatchlist(
        name=watchlist_name,
        watchlist_entries=tickers_watchlist_entries,
        group_name="default",
        order_index=9999,
    )
    watchlist.upload(session)
    logging.info(f"Successfully uploaded watchlist '{watchlist_name}' with {len(symbols_list)} tickers.")


def get_ticker_universe(session):
    """Builds and uploads an optimized Ticker Universe watchlist to Tastytrade.
    The process involves:
      1. Fetching all active equities and filtering for those that are optionable.
      2. Fetching market metrics for these symbols and filtering by liquidity and type (ETF/stock).
      3. Further filtering symbols based on having a sufficient number of option expirations
         within a defined future window.
      4. Uploading the final list of ticker symbols as a private watchlist to Tastytrade.
    """
    logging.info("Starting the process to build and upload Ticker Universe watchlist.")

    # Step 1: Fetch active optionable equities
    optionable_df = _fetch_active_optionable_equities(session)
    if optionable_df.empty:
        logging.error("No active optionable equities found. Aborting.")
        return

    # Step 2: Fetch market metrics and filter by liquidity/type
    filtered_metrics_df, _ = _get_metrics_and_filter_by_liquidity(session, optionable_df)
    if filtered_metrics_df.empty:
        logging.error("No symbols passed the liquidity and type filter. Aborting.")
        return

    # Step 3: Filter for sufficient expirations in target window
    final_ticker_symbols = _filter_by_option_expirations(filtered_metrics_df)
    # Sort the final list of symbols alphabetically
    final_ticker_symbols.sort()
    if not final_ticker_symbols:
        logging.error("No symbols passed the option expiration filter. Aborting watchlist upload.")
        return

    # Step 4: Prepare and upload the watchlist
    _upload_watchlist_to_tastytrade(session, final_ticker_symbols, WATCHLIST_NAME)

    logging.info("Ticker Universe watchlist update process completed.")


# Example usage (assuming 'session' is an authenticated Tastytrade session object)
# if __name__ == "__main__":
#     from Scripts.session_login import session_login
#     logging.info("Attempting to log in using session_login...")
#     try:
#         # Use your session_login function to get the session
#         session = session_login()

#         logging.info("Successfully created Tastytrade session via session_login.")
#         get_ticker_universe(session)

#     except Exception as e:
#         logging.error(f"An error occurred during login or script execution: {e}", exc_info=True)
