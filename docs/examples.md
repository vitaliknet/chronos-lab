# Examples

```python
from chronos_lab.sources import ohlcv_from_yfinance, ohlcv_from_arcticdb
from chronos_lab.analysis import detect_ohlcv_anomalies

# Option 1: Fetch from yfinance and detect anomalies
symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA']
ohlcv = ohlcv_from_yfinance(symbols=symbols, period='1y')

# Detect anomalies with defaults (parallel processing enabled by default)
anomalies = detect_ohlcv_anomalies(ohlcv)

# Filter to just the anomalous days
detected = anomalies[anomalies['is_anomaly']]

# Option 2: From ArcticDB
ohlcv = ohlcv_from_arcticdb(
    symbols=symbols,
    period='1y'
)
anomalies = detect_ohlcv_anomalies(ohlcv)

# Option 3: Dict format input (if you prefer working with dicts)
ohlcv_dict = ohlcv_from_yfinance(symbols=symbols, period='1y', output_dict=True)
anomalies_dict = detect_ohlcv_anomalies(ohlcv_dict)

# Access individual symbol results
aapl_anomalies = anomalies_dict['AAPL']

# Option 4: Customize features and parameters
anomalies = detect_ohlcv_anomalies(
    ohlcv,
    features=['returns', 'volume_change', 'high_low_range', 'volatility'],
    contamination=0.05,  # Expect 5% anomalies
    use_adjusted=True,
    parallel=True,
    n_estimators=200,  # sklearn parameter
    max_samples=512  # sklearn parameter
)
```

## Next Steps

- Review [API Reference](api/index.md) for detailed function documentation
- Check [Configuration](configuration.md) for setup options
- Explore [Getting Started](getting-started.md) for basic workflows
