import asyncio
import datetime
import logging
import os
import statistics
import sys
from decimal import Decimal

# Add the parent directory (Scripts) to sys.path to allow imports from it.
_CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.abspath(os.path.join(_CURRENT_FILE_DIR, ".."))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import pandas as pd
from get_ticker_universe import get_ticker_universe
from session_login import session_login
from shared_tasty_utils import (  # Constants
    MAX_WORKERS,
    REQUEST_TIMEOUT,
    fetch_market_data_efficient,
    get_option_chain_async,
)
from tastytrade import DXLinkStreamer
from tastytrade.dxfeed import Quote
from tastytrade.market_data import get_market_data_by_type
from tastytrade.watchlists import PrivateWatchlist, PublicWatchlist
from tqdm import tqdm

# Constants
DELTA_THRESHOLD = Decimal("0.16")
MIN_OPEN_INTEREST = 200
MIN_STOCK_PRICE = 5
MIN_VALID_OPTIONS = 10
MIN_EXPIRATIONS_COUNT = 3
MAX_AVG_SPREAD_PERCENTAGE = 0.5
VIX_SYMBOL = "VIX"
WATCHLIST_NAME = "MyWatchlist"
FORCED_TICKERS = ["SPY", "QQQ"]  # Add any tickers you want to always keep in the watchlist
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# _SCRIPTS_DIR is c:\Users\Joe\Desktop\Stock\Scripts. We want Data next to Scripts.
DATA_DIR = os.path.join(os.path.dirname(_SCRIPTS_DIR), "Data")
TICKER_REMOVAL_FILE = os.path.join(DATA_DIR, "ticker_removal_watch.txt")
TICKER_ADD_FILE = os.path.join(DATA_DIR, "ticker_add_watch.txt")

# Re-define MAX_WORKERS if the dynamic calculation is preferred over the shared constant
# MAX_WORKERS = min(multiprocessing.cpu_count() * 2, 16) # This would override the imported MAX_WORKERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s",
)

# Default structure for ticker metrics when processing fails or filters are not met
EMPTY_TICKER_METRICS = {
    "avg_spread_percentage": float("nan"),
    "median_spread_percentage": float("nan"),
    "valid_options": float("nan"),  # Represents avg_options_per_expiration
    "avg_options_per_expiration": 0,
    "per_expiration_data": {},
    "expirations_count": 0,
}


def initialize_data_directory():
    """Initialize the data directory and create empty ticker files if they don't exist."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        logging.info(f"Created directory: {DATA_DIR}")

    for file_path in [TICKER_REMOVAL_FILE, TICKER_ADD_FILE]:
        if not os.path.exists(file_path):
            with open(file_path, "w") as f:
                pass  # Create an empty file if it doesn't exist
            logging.info(f"Created empty file: {file_path}")


def read_ticker_file(path):
    """Read tickers from a file."""
    initialize_data_directory()
    try:
        with open(path) as file:
            return [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        logging.warning(f"File {path} not found, returning empty list.")
        return []


def write_ticker_file(path, tickers):
    """Write tickers to a file."""
    initialize_data_directory()
    with open(path, "w") as file:
        file.writelines(f"{item}\n" for item in sorted(tickers))
    logging.info(f"Updated file: {path} with {len(tickers)} tickers")


def is_futures_symbol(ticker):
    """Check if a symbol is a futures symbol (starts with '/')."""
    return ticker and ticker.startswith("/")


async def get_stock_prices_batch(session, tickers, retries=3, delay=5):
    """Fetch stock prices for a batch of tickers in one API call with retries."""
    # Note: For more advanced error handling with problematic symbols in a batch,
    # future enhancements could include dynamic chunk size reduction or a binary search approach
    # if the API frequently fails due to individual "poison pill" symbols within a batch.
    loop = asyncio.get_event_loop()
    for attempt in range(retries):
        try:
            data = await loop.run_in_executor(None, lambda: get_market_data_by_type(session, equities=tickers))
            return {d.symbol: d.mid for d in data if d.mid is not None}
        except Exception:
            logging.error(f"Attempt {attempt + 1}/{retries} failed for fetching stock prices for batch: {tickers}", exc_info=True)
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                logging.exception(f"Failed to fetch stock prices for batch after {retries} attempts: {tickers}")
                return {}


async def process_ticker_batch_parallel(session, tickers_batch, min_exp, max_exp, stock_prices):
    """Process a batch of tickers with a semaphore to limit concurrency."""
    semaphore = asyncio.Semaphore(MAX_WORKERS)

    async def bounded_process_ticker(ticker):
        async with semaphore:
            # Skip processing for futures symbols
            if is_futures_symbol(ticker):
                return {
                    "entry": ticker,
                    "stock_price": float("nan"),
                    "avg_spread_percentage": float("nan"),
                    "median_spread_percentage": float("nan"),
                    "valid_options": float("nan"),
                    "expirations_count": 0,
                    "avg_options_per_expiration": 0,
                    "per_expiration_data": {},
                    "is_futures": True,  # Flag to identify futures symbols
                }
            stock_price = stock_prices.get(ticker)
            return await process_ticker(session, ticker, min_exp, max_exp, stock_price)

    tasks = [bounded_process_ticker(ticker) for ticker in tickers_batch]
    return await asyncio.gather(*tasks, return_exceptions=True)


async def _get_initial_option_data(session, ticker, min_exp, max_exp):
    """Fetches the option chain for a ticker and performs initial filtering based on expiration dates.
    Prepares symbol lists for market data fetching.
    Corresponds to Sections 1 & 2 of the original process_ticker logic.
    """
    chain = await get_option_chain_async(session, ticker)

    # Filter expirations within the desired date range
    filtered_chain = {k: v for k, v in chain.items() if isinstance(k, datetime.date) and min_exp <= k <= max_exp}

    if not filtered_chain:
        return None, None, None  # No valid expirations

    # Prepare symbols for market data fetching
    symbols_with_exp = [
        {
            "symbol": option.symbol,
            "streamer_symbol": option.streamer_symbol,
            "expiration_date": exp_date,
        }
        for exp_date, options in filtered_chain.items()
        for option in options
    ]

    streamer_symbols = list({item["streamer_symbol"] for item in symbols_with_exp})
    symbol_to_expiration = {item["streamer_symbol"]: item["expiration_date"] for item in symbols_with_exp}

    return filtered_chain, streamer_symbols, symbol_to_expiration


async def _fetch_and_filter_option_market_data(session, streamer_symbols):
    """Fetches Greeks and Summary market data for the given option symbols and filters them.
    Corresponds to Sections 3 & 4 of the original process_ticker logic.
    """
    if not streamer_symbols:
        return {}

    all_symbol_data = await fetch_market_data_efficient(session, streamer_symbols)

    # Filter options based on Greeks (delta) and Summary (open interest) data
    filtered_data = {
        symbol: data
        for symbol, data in all_symbol_data.items()
        if (
            data["greeks"] is not None
            and data["greeks"].delta is not None
            and Decimal("0") <= abs(data["greeks"].delta) <= DELTA_THRESHOLD
            and data["summary"] is not None
            and getattr(data["summary"], "open_interest", 0) >= MIN_OPEN_INTEREST
        )
    }
    return filtered_data


async def _stream_and_organize_spreads(session, filtered_data, stock_price, symbol_to_expiration):
    """Streams quotes for filtered options, calculates spreads, and organizes them by expiration.
    Corresponds to Section 5 and part of Section 6 of the original process_ticker logic.
    """
    if not filtered_data:
        return {}

    spreads = await stream_quotes_with_spread_efficient(session, filtered_data, stock_price)

    spreads_by_expiration = {}
    for symbol, spread_percentage in spreads.items():
        if spread_percentage is not None and not pd.isna(spread_percentage):
            exp_date = symbol_to_expiration.get(symbol)
            if exp_date:
                spreads_by_expiration.setdefault(exp_date, []).append(float(spread_percentage))
    return spreads_by_expiration


def _perform_final_calculations_and_filtering(ticker, stock_price, spreads_by_expiration_input):
    """Calculates per-expiration and overall statistics from the spreads.
    Applies final filters based on these statistics.
    This function is synchronous and assumes prior async data fetching is complete.
    """
    base_result = {
        "entry": ticker,
        "stock_price": stock_price,
        **EMPTY_TICKER_METRICS,  # Start with empty metrics
    }

    # Early filter: Check if enough expirations exist (based on actual spreads found)
    if len(spreads_by_expiration_input) < MIN_EXPIRATIONS_COUNT:
        base_result["expirations_count"] = len(spreads_by_expiration_input)
        return base_result

    per_expiration_data = {}
    total_valid_options = 0
    total_avg_spread = 0

    for exp_date, exp_spreads in spreads_by_expiration_input.items():
        if exp_spreads:  # Ensure there are spreads for this expiration
            avg_spread = statistics.mean(exp_spreads)
            median_spread = statistics.median(exp_spreads)  # Calculated per expiration
            option_count = len(exp_spreads)
            total_valid_options += option_count
            total_avg_spread += avg_spread

            per_expiration_data[exp_date.isoformat()] = {
                "avg_spread_percentage": avg_spread,
                "median_spread_percentage": median_spread,
                "valid_options": option_count,
            }

    # This count is based on expirations that *had valid spreads after streaming*
    expirations_count_with_data = len(per_expiration_data)

    if expirations_count_with_data == 0:  # Should ideally be caught by MIN_EXPIRATIONS_COUNT if > 0
        base_result["expirations_count"] = len(spreads_by_expiration_input)  # Original count of expirations with some spreads
        return base_result

    avg_options_per_expiration = total_valid_options / expirations_count_with_data
    avg_of_avg_spreads = total_avg_spread / expirations_count_with_data

    # Update base_result with calculated values before final filtering
    base_result.update(
        {
            "avg_spread_percentage": avg_of_avg_spreads,
            "valid_options": avg_options_per_expiration,  # This is avg_options_per_expiration
            "expirations_count": expirations_count_with_data,
            "avg_options_per_expiration": avg_options_per_expiration,
            "per_expiration_data": per_expiration_data,
        },
    )

    # Final filter: Check average valid options per expiration and average spread percentage
    if avg_options_per_expiration < MIN_VALID_OPTIONS or avg_of_avg_spreads >= MAX_AVG_SPREAD_PERCENTAGE:
        # Return current calculations, median_spread_percentage remains NaN as per original logic for this path
        return base_result

    # Calculate overall median spread from all valid options if all filters passed
    all_spreads = [spread for exp_spreads_list in spreads_by_expiration_input.values() for spread in exp_spreads_list]
    overall_median = statistics.median(all_spreads) if all_spreads else float("nan")

    return {  # Ticker passed all filters
        **base_result,  # Contains most up-to-date values
        "median_spread_percentage": overall_median,
    }


async def process_ticker(session, ticker, min_exp, max_exp, stock_price):
    """Process a single ticker: fetch data, apply filters, and calculate metrics."""
    # Base structure for returns, especially early exits or errors
    result_template = {
        "entry": ticker,
        "stock_price": stock_price if stock_price is not None else float("nan"),
        **EMPTY_TICKER_METRICS,
    }

    try:
        # Initial stock price check (futures are handled before this function is called)
        if stock_price is None or stock_price < MIN_STOCK_PRICE:
            return result_template  # stock_price is already set correctly

        # Step 1: Get initial option data (chain, streamer symbols, symbol to expiration map)
        # filtered_chain_data contains {exp_date: [Option]}
        filtered_chain_data, streamer_symbols, symbol_to_expiration = await _get_initial_option_data(session, ticker, min_exp, max_exp)
        if not streamer_symbols:  # No valid expirations or options from chain
            result_template["expirations_count"] = 0  # No expirations met date range or had options
            return result_template

        # Step 2: Fetch and filter market data (Greeks, Summary)
        # filtered_option_details contains {streamer_symbol: {"greeks": Greeks, "summary": Summary}}
        filtered_option_details = await _fetch_and_filter_option_market_data(session, streamer_symbols)
        if not filtered_option_details:  # No options met Greeks/Summary criteria
            result_template["expirations_count"] = len(filtered_chain_data) if filtered_chain_data else 0
            return result_template

        # Step 3: Stream quotes, calculate spreads, and organize them by expiration
        # spreads_by_expiration_result contains {exp_date: [spread_percentage_float]}
        spreads_by_expiration_result = await _stream_and_organize_spreads(
            session,
            filtered_option_details,
            stock_price,
            symbol_to_expiration,
        )

        # Step 4: Perform final calculations and filtering (synchronous function)
        return _perform_final_calculations_and_filtering(ticker, stock_price, spreads_by_expiration_result)

    except Exception as e:
        logging.error(f"Error processing ticker {ticker}: {e}", exc_info=True)
        # Ensure stock_price is correctly set in the template if it was modified or an error occurred early
        result_template["stock_price"] = stock_price if "stock_price" in locals() and stock_price is not None else float("nan")
        return result_template


async def stream_quotes_with_spread_efficient(session, filtered_data, stock_price):
    """Stream quotes and calculate spread percentages."""
    symbols = list(filtered_data.keys())
    spreads = {}

    if not symbols or stock_price is None:
        return spreads

    # Optimal batch size for Quote subscriptions
    optimal_batch_size = min(200, len(symbols))
    symbol_batches = [symbols[i : i + optimal_batch_size] for i in range(0, len(symbols), optimal_batch_size)]

    # Limit concurrent DXLinkStreamer instances for quotes
    semaphore = asyncio.Semaphore(MAX_WORKERS)

    async def process_quote_batch(batch):
        async with semaphore:
            return await stream_quotes_batch(session, batch, stock_price)

    batch_results = await asyncio.gather(*[process_quote_batch(batch) for batch in symbol_batches], return_exceptions=True)

    for batch_spreads in batch_results:
        if isinstance(batch_spreads, dict):
            spreads.update(batch_spreads)
        elif isinstance(batch_spreads, Exception):
            logging.error(f"A batch in stream_quotes_with_spread_efficient failed: {batch_spreads!s}", exc_info=batch_spreads)

    return spreads


async def stream_quotes_batch(session, symbols, stock_price):
    """Stream quotes for a batch of symbols."""
    spreads = {}

    if not symbols:
        return spreads

    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Quote, symbols)

        pending_symbols = set(symbols)
        start_time = asyncio.get_event_loop().time()

        try:
            while pending_symbols and (asyncio.get_event_loop().time() - start_time < REQUEST_TIMEOUT):
                try:
                    quotes_received = 0
                    max_quotes_per_batch = 5

                    while quotes_received < max_quotes_per_batch and pending_symbols:
                        event = await asyncio.wait_for(streamer.listen(Quote).__anext__(), 0.1)
                        quotes_received += 1

                        symbol = event.event_symbol
                        if symbol in pending_symbols:
                            if event.bid_price is not None and event.ask_price is not None:
                                spread = Decimal(str(event.ask_price)) - Decimal(str(event.bid_price))
                                spreads[symbol] = (spread / stock_price) * 100
                            else:
                                spreads[symbol] = None  # Mark as None if bid/ask is missing
                            pending_symbols.remove(symbol)

                except (asyncio.TimeoutError, StopAsyncIteration):
                    await asyncio.sleep(0.005)  # Brief pause if no event, then retry loop

        except Exception:  # Catch broader errors in the DXLink interaction
            logging.error("Error during stream_quotes_batch event loop", exc_info=True)

    # For any symbols that were not processed (e.g., timeout before all quotes received),
    # ensure they have an entry in spreads, marked as None.
    for symbol in pending_symbols:
        if symbol not in spreads:
            spreads[symbol] = None

    return spreads


def extract_symbols_from_entries(entries):
    """Extract symbols from watchlist entries."""
    symbols = set()
    for entry in entries:
        symbol = entry.get("symbol") if isinstance(entry, dict) else getattr(entry, "symbol", None)
        if symbol:
            symbols.add(symbol)
    return symbols


def get_futures_symbols(futures_entries):
    """Extract symbols from futures entries."""
    futures_symbols = set()
    for entry in futures_entries:
        symbol = entry.get("symbol") if isinstance(entry, dict) else getattr(entry, "symbol", None)
        if symbol and is_futures_symbol(symbol):
            futures_symbols.add(symbol)
    return futures_symbols


async def update_watchlist(session, valid_tickers, futures_with_options):
    """Update the watchlist with valid tickers and futures symbols."""
    # Extract futures symbols
    all_futures_symbols = get_futures_symbols(futures_with_options)
    logging.info(f"Found {len(all_futures_symbols)} futures symbols starting with '/' from public watchlist.")

    # Read ticker tracking files
    ticker_removal_watch = read_ticker_file(TICKER_REMOVAL_FILE)
    ticker_add_watch = read_ticker_file(TICKER_ADD_FILE)

    # Define symbols that are managed specially (VIX and FORCED_TICKERS).
    # These symbols will always be included in the watchlist and are exempt from the usual add/remove logic.
    special_managed_symbols = {VIX_SYMBOL} | set(FORCED_TICKERS)
    # Get current watchlist
    try:
        watchlists = PrivateWatchlist.get(session, WATCHLIST_NAME)
        current_watchlist_symbols = extract_symbols_from_entries(watchlists.watchlist_entries)
    except Exception:
        logging.warning(f"Could not retrieve existing watchlist '{WATCHLIST_NAME}'. Assuming it's empty or needs creation.", exc_info=True)
        current_watchlist_symbols = set()

    # old_list_managed: Tickers currently in the watchlist, excluding futures and special symbols (e.g., VIX, FORCED_TICKERS).
    # These are the non-special, non-future tickers currently being managed and subject to removal if they fail liquidity checks.
    old_list_managed = [ticker for ticker in current_watchlist_symbols if not is_futures_symbol(ticker) and ticker not in special_managed_symbols]

    # valid_tickers_for_management: Tickers that passed all liquidity criteria in the current analysis run,
    # excluding futures and special symbols. These are candidates for being in the final watchlist (either retained or newly added).
    valid_tickers_for_management = [ticker for ticker in valid_tickers if not is_futures_symbol(ticker) and ticker not in special_managed_symbols]

    # Identify tickers for definitive removal: these were on the 'ticker_removal_watch.txt' (failed the *previous* run)
    # AND are *not* in 'valid_tickers_for_management' (failed the *current* run again).
    # Special symbols (VIX, FORCED_TICKERS) are exempt from this removal logic.
    two_time_missing = [value for value in ticker_removal_watch if value not in valid_tickers_for_management and value not in special_managed_symbols]

    # Update the list of currently managed tickers by removing those that failed twice.
    old_list_after_removals = [value for value in old_list_managed if value not in two_time_missing]

    # Identify tickers for the 'removal watch' list (i.e., first-time failure in this cycle):
    # These were in 'old_list_after_removals' (were considered stable or on first warning) but are NOT in 'valid_tickers_for_management' (failed current run).
    # They are kept in the watchlist for one more cycle and added to `TICKER_REMOVAL_FILE`. Special symbols are exempt.
    new_ticker_removal_watch = [
        item for item in old_list_after_removals if item not in valid_tickers_for_management and item not in special_managed_symbols
    ]
    write_ticker_file(TICKER_REMOVAL_FILE, new_ticker_removal_watch)

    # Identify potential new tickers: these are in 'valid_tickers_for_management' (passed current run)
    # but are NOT in 'old_list_after_removals' (meaning they are new, or were previously removed and are now valid again).
    # Special symbols are exempt from this specific part of the addition logic (they are added unconditionally later).
    tickers_to_possibly_add = [
        item for item in valid_tickers_for_management if item not in old_list_after_removals and item not in special_managed_symbols
    ]

    # Tickers to definitively add to the watchlist: these are 'tickers_to_possibly_add' (valid now and not currently stable)
    # AND were on the 'ticker_add_watch.txt' (meaning they were identified as candidates in a previous run and have now passed analysis again).
    tickers_to_add = [ticker for ticker in tickers_to_possibly_add if ticker in ticker_add_watch]

    # Identify tickers for the 'add watch' list (i.e., first-time candidates for addition in this cycle):
    # These are 'tickers_to_possibly_add' (valid now and not currently stable) but were NOT on 'ticker_add_watch.txt' yet.
    # They are added to `TICKER_ADD_FILE` for future consideration if they pass analysis again. Special symbols are exempt.
    tickers_to_possibly_add_in_future = [
        item for item in tickers_to_possibly_add if item not in tickers_to_add and item not in special_managed_symbols
    ]
    write_ticker_file(TICKER_ADD_FILE, tickers_to_possibly_add_in_future)

    # 1. Start with all non-future, non-special tickers that passed the current analysis.
    final_watchlist_symbols = set(valid_tickers_for_management)
    # 2. Add tickers confirmed for addition (passed analysis and were on add_watch, or passed twice).
    final_watchlist_symbols.update(tickers_to_add)
    # 3. Add tickers on their first removal warning (to keep them for one more cycle, even if they failed this run).
    final_watchlist_symbols.update(new_ticker_removal_watch)
    # 4. Add all identified futures symbols.
    final_watchlist_symbols.update(all_futures_symbols)
    # 5. Ensure VIX and all FORCED_TICKERS are included, regardless of analysis results or prior watch status.
    final_watchlist_symbols.update(special_managed_symbols)

    # Create watchlist entries (ensure none are None or empty)
    tickers_watchlist = [{"symbol": symbol} for symbol in sorted(list(final_watchlist_symbols)) if symbol]

    # Safety check: Ensure VIX_SYMBOL is in the final list.
    # This should ideally be redundant if VIX_SYMBOL is correctly part of special_managed_symbols.
    vix_included = any(entry.get("symbol") == VIX_SYMBOL for entry in tickers_watchlist)
    if not vix_included and VIX_SYMBOL:  # Check VIX_SYMBOL is not empty
        logging.warning(f"{VIX_SYMBOL} was somehow missing, adding it back to watchlist entries.")
        tickers_watchlist.append({"symbol": VIX_SYMBOL})
        tickers_watchlist.sort(key=lambda x: x["symbol"])

    # Delete old watchlist
    try:
        PrivateWatchlist.remove(session, WATCHLIST_NAME)
    except Exception:
        logging.info(f"Watchlist '{WATCHLIST_NAME}' not found for removal, or removal failed. Proceeding to create/update.")

    # Save new watchlist
    watchlist = PrivateWatchlist(
        name=WATCHLIST_NAME,
        watchlist_entries=tickers_watchlist,
        group_name="default",  # Ensure this is a valid group name if your account uses specific groups
        order_index=9999,  # A high number to place it at the end, adjust as needed
    )
    watchlist.upload(session)
    logging.info(f"Updated watchlist '{WATCHLIST_NAME}' with {len(tickers_watchlist)} tickers.")
    logging.info(f"Futures symbols in watchlist: {len(all_futures_symbols)}")
    logging.info(f"Forced symbols in watchlist (incl. VIX if applicable): {', '.join(sorted(list(special_managed_symbols)))}")
    logging.info(f"Tickers removed (missed twice): {two_time_missing}")
    logging.info(f"Tickers on first-time removal warning: {new_ticker_removal_watch}")
    logging.info(f"Tickers added this cycle: {tickers_to_add}")
    logging.info(f"Tickers to watch for future addition: {tickers_to_possibly_add_in_future}")


async def _get_initial_ticker_lists(session):
    """Fetches the list of futures with options and the main ticker universe for analysis.

    Returns:
        tuple: (futures_with_options_entries, analysis_candidate_tickers)

    """
    # Get futures with options
    try:
        public_watchlist = PublicWatchlist.get(session, "Futures: With Options")
        futures_with_options = public_watchlist.watchlist_entries
        # futures_symbols = get_futures_symbols(futures_with_options) # This is done in update_watchlist
        logging.info(f"Found {len(futures_with_options)} entries in 'Futures: With Options' watchlist.")
    except Exception:
        logging.error("Error getting 'Futures: With Options' watchlist. Proceeding with empty list.", exc_info=True)
        futures_with_options = []

    # Get ticker universe
    # These are non-future symbols that are candidates for the main liquidity analysis.
    # VIX and FORCED_TICKERS are handled by `update_watchlist` for inclusion;
    # they will be analyzed if present in TickerUniverse or get_ticker_universe.

    try:
        watchlists = PrivateWatchlist.get(session, "TickerUniverse")
        tickers_watchlist = extract_symbols_from_entries(watchlists.watchlist_entries)
        # We analyze all non-future symbols from TickerUniverse.
        # Special handling for VIX/FORCED_TICKERS is for final watchlist assembly.
        tickers_watchlist = [ticker for ticker in tickers_watchlist if not is_futures_symbol(ticker)]
        logging.info(f"Found {len(tickers_watchlist)} tickers in 'TickerUniverse' watchlist for analysis.")
    except Exception:
        logging.warning("Error getting 'TickerUniverse' watchlist. Falling back to get_ticker_universe().", exc_info=True)
        tickers_watchlist = get_ticker_universe(session)
        tickers_watchlist = [ticker for ticker in tickers_watchlist if not is_futures_symbol(ticker)]
        logging.info(f"Using alternate ticker universe with {len(tickers_watchlist)} tickers for analysis.")
    return futures_with_options, tickers_watchlist


async def main():
    # Set up optimized event loop policy if on Windows
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    session = session_login()
    logging.info(f"Successfully logged in to TastyTrade API. Using {MAX_WORKERS} concurrent workers.")

    futures_with_options, tickers_to_analyze = await _get_initial_ticker_lists(session)

    # Date range for option expiration
    today = datetime.date.today()
    min_exp = today + datetime.timedelta(days=5)
    max_exp = today + datetime.timedelta(days=83)
    logging.info(f"Analyzing options with expiration between {min_exp} and {max_exp}")
    logging.info(
        f"Applying filters: stock_price >= ${MIN_STOCK_PRICE}, avg_options_per_expiration >= {MIN_VALID_OPTIONS}, "
        f"expirations_count >= {MIN_EXPIRATIONS_COUNT}, avg_spread_percentage < {MAX_AVG_SPREAD_PERCENTAGE}%",
    )

    # Optimize batch size for fetching stock prices and processing tickers
    # MAX_WORKERS * 2 is a heuristic, adjust based on typical API response times and processing load per ticker
    optimal_batch_size = MAX_WORKERS * 2
    ticker_batches = [list(tickers_to_analyze)[i : i + optimal_batch_size] for i in range(0, len(tickers_to_analyze), optimal_batch_size)]

    # Process tickers
    all_results = []

    with tqdm(total=len(tickers_to_analyze), desc="Processing tickers") as pbar:
        for batch in ticker_batches:
            # Fetch stock prices for the entire batch once
            stock_prices = await get_stock_prices_batch(session, batch)  # Retries are handled inside
            # Process the batch of tickers concurrently
            batch_results = await process_ticker_batch_parallel(session, batch, min_exp, max_exp, stock_prices)
            for r in batch_results:
                if isinstance(r, Exception):  # Should not happen if process_ticker_batch_parallel handles exceptions
                    logging.error(f"Unhandled exception from process_ticker_batch_parallel for a ticker: {r!s}", exc_info=r)
                elif r is not None:  # Ensure result is not None
                    all_results.append(r)
            pbar.update(len(batch))

    # Create and filter DataFrame
    if not all_results:
        logging.warning("No results were obtained from ticker processing. Watchlist will not be updated with new analytics data.")
        results_df = pd.DataFrame(columns=list(EMPTY_TICKER_METRICS.keys()) + ["entry", "stock_price"])  # Ensure schema
    else:
        results_df = pd.DataFrame(all_results)

    # Apply final filters to the collected results to determine "valid_tickers"
    # Ensure columns exist before filtering, especially if results_df could be empty or malformed
    required_cols = ["avg_spread_percentage", "valid_options", "expirations_count", "stock_price", "entry"]
    if not all(col in results_df.columns for col in required_cols):
        logging.error(f"Results DataFrame is missing one or more required columns: {required_cols}. Cannot filter.")
        filtered_results_df = pd.DataFrame(columns=results_df.columns)  # Empty DF with same schema
    else:
        filtered_results_df = results_df[
            (~pd.isna(results_df["avg_spread_percentage"]))
            & (~pd.isna(results_df["valid_options"]))
            & (results_df["valid_options"] >= MIN_VALID_OPTIONS)  # This refers to avg_options_per_expiration
            & (results_df["expirations_count"] >= MIN_EXPIRATIONS_COUNT)
            & (results_df["avg_spread_percentage"] < MAX_AVG_SPREAD_PERCENTAGE)
            & (results_df["stock_price"] >= MIN_STOCK_PRICE)
        ].sort_values("avg_spread_percentage")

    # Extract valid tickers for watchlist update
    valid_tickers = filtered_results_df["entry"].tolist() if not filtered_results_df.empty else []

    # Summary statistics
    logging.info("\n--- Analysis Summary ---")
    logging.info(f"Total tickers submitted for processing: {len(tickers_to_analyze)}")
    logging.info(f"Total tickers for which results were obtained: {len(results_df)}")
    if not results_df.empty and "stock_price" in results_df.columns:
        logging.info(
            f"Tickers with stock price >= ${MIN_STOCK_PRICE}: {len(results_df[results_df['stock_price'] >= MIN_STOCK_PRICE])}",
        )
    else:
        logging.info(f"Tickers with stock price >= ${MIN_STOCK_PRICE}: 0 (or data unavailable)")
    logging.info(f"Tickers meeting all option liquidity criteria: {len(filtered_results_df)}")

    # Update watchlist
    await update_watchlist(session, valid_tickers, futures_with_options)
    logging.info("\n--- Analysis Complete ---")


if __name__ == "__main__":
    initialize_data_directory()  # Ensure data directory and tracking files exist before starting
    asyncio.run(main())
