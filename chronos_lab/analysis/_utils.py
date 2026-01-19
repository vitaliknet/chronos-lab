import pandas as pd
from typing import Dict, List, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from chronos_lab import logger


def process_by_symbol(
    ohlcv_input: pd.DataFrame | Dict[str, pd.DataFrame],
    executor_fn: Callable[[pd.DataFrame], pd.DataFrame],
    parallel: bool = True,
    max_workers: int | None = None
) -> pd.DataFrame | Dict[str, pd.DataFrame]:

    input_is_dict = isinstance(ohlcv_input, dict)

    if input_is_dict:
        symbol_dfs = ohlcv_input
        symbols = list(symbol_dfs.keys())
    else:
        if isinstance(ohlcv_input.index, pd.MultiIndex):
            level_name = ohlcv_input.index.names[1] if len(ohlcv_input.index.names) > 1 else 1
            symbol_dfs = {
                symbol: df.droplevel(level_name)
                for symbol, df in ohlcv_input.groupby(level=level_name)
            }
            symbols = list(symbol_dfs.keys())
        else:
            symbol_dfs = {'_single_': ohlcv_input}
            symbols = ['_single_']

    logger.info(f'Processing {len(symbols)} symbols (parallel={parallel})')

    results = {}

    if parallel and len(symbols) > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(executor_fn, df): symbol
                for symbol, df in symbol_dfs.items()
            }

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    results[symbol] = result
                    logger.info(f'Completed processing for {symbol}')
                except Exception as e:
                    logger.error(f'Error processing {symbol}: {e}')
                    raise
    else:
        for symbol, df in symbol_dfs.items():
            try:
                result = executor_fn(df)
                results[symbol] = result
                logger.info(f'Completed processing for {symbol}')
            except Exception as e:
                logger.error(f'Error processing {symbol}: {e}')
                raise

    if symbols == ['_single_']:
        return results['_single_']

    if input_is_dict:
        return results
    else:
        combined_dfs = []
        for symbol, df in results.items():
            df_copy = df.copy()
            df_copy['_symbol_'] = symbol
            combined_dfs.append(df_copy)

        combined = pd.concat(combined_dfs)
        combined = combined.set_index('_symbol_', append=True)

        if ohlcv_input.index.names[1]:
            combined.index = combined.index.set_names(
                [ohlcv_input.index.names[0], ohlcv_input.index.names[1]]
            )

        return combined
