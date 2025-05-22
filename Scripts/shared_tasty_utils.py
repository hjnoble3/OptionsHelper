import asyncio
import logging

from tastytrade import DXLinkStreamer
from tastytrade.dxfeed import Greeks, Summary
from tastytrade.instruments import get_option_chain

# Constants moved from tradeable_tickers.py
BATCH_SIZE = 5000  # Used by fetch_market_data_efficient
MAX_WORKERS = 16  # Default, can be overridden by specific script if needed. tradeable_tickers.py recalculates it.
# For MAX_WORKERS, tradeable_tickers.py uses min(multiprocessing.cpu_count() * 2, 16).
# If this dynamic calculation is crucial for shared_tasty_utils, it would need multiprocessing import.
# For now, using the common upper limit.
REQUEST_TIMEOUT = 3  # Used by fetch_market_data_batch and stream_quotes_batch (latter is still in tradeable_tickers)


async def get_option_chain_async(session, ticker):
    """Get option chain asynchronously."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: get_option_chain(session, ticker))


async def get_next_event(streamer, event_type):
    """Get next event with timeout."""
    try:
        event = await asyncio.wait_for(streamer.listen(event_type).__anext__(), 0.05)
        return (event, event_type)
    except (asyncio.TimeoutError, StopAsyncIteration):
        return None


async def fetch_market_data_batch(session, streamer_symbols):
    """Fetch market data for a batch of symbols."""
    symbol_data = {symbol: {"greeks": None, "summary": None} for symbol in streamer_symbols}

    if not streamer_symbols:
        return symbol_data

    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Greeks, streamer_symbols)
        await streamer.subscribe(Summary, streamer_symbols)

        received_greeks = set()
        received_summary = set()
        start_time = asyncio.get_event_loop().time()

        try:
            while (len(received_greeks) < len(streamer_symbols) or len(received_summary) < len(streamer_symbols)) and (
                asyncio.get_event_loop().time() - start_time < REQUEST_TIMEOUT
            ):
                greek_task = asyncio.create_task(get_next_event(streamer, Greeks))
                summary_task = asyncio.create_task(get_next_event(streamer, Summary))

                done, pending = await asyncio.wait(
                    [greek_task, summary_task],
                    timeout=0.1,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in pending:
                    task.cancel()

                for task in done:
                    try:
                        result = task.result()
                        if result:
                            event, event_type = result
                            symbol = event.event_symbol

                            if event_type == Greeks and symbol not in received_greeks:
                                symbol_data[symbol]["greeks"] = event
                                received_greeks.add(symbol)

                            elif event_type == Summary and symbol not in received_summary:
                                symbol_data[symbol]["summary"] = event
                                received_summary.add(symbol)
                    except Exception:
                        logging.debug(
                            f"Error processing event in fetch_market_data_batch for {symbol if 'symbol' in locals() else 'unknown'}",
                            exc_info=True,
                        )
                        # Individual event processing error
        except Exception:
            logging.error("Error during fetch_market_data_batch event loop", exc_info=True)

    return symbol_data


async def fetch_market_data_efficient(session, streamer_symbols):
    """Efficiently fetch market data using batching."""
    if not streamer_symbols:
        return {}

    # Optimal batch size for DXLink subscriptions (empirically, around 500 is a good upper limit per subscription type)
    optimal_batch_size = min(BATCH_SIZE, 500)
    symbol_batches = [streamer_symbols[i : i + optimal_batch_size] for i in range(0, len(streamer_symbols), optimal_batch_size)]

    # Limit concurrent DXLinkStreamer instances or heavy subscription calls
    # Note: MAX_WORKERS here is from this shared_tasty_utils.py
    semaphore = asyncio.Semaphore(MAX_WORKERS)

    async def process_batch(batch):
        async with semaphore:
            return await fetch_market_data_batch(session, batch)

    batch_results = await asyncio.gather(*[process_batch(batch) for batch in symbol_batches], return_exceptions=True)

    all_symbol_data = {}
    for batch_data in batch_results:
        if isinstance(batch_data, dict):
            all_symbol_data.update(batch_data)
        elif isinstance(batch_data, Exception):
            logging.error(f"A batch in fetch_market_data_efficient failed: {batch_data!s}", exc_info=batch_data)

    return all_symbol_data
