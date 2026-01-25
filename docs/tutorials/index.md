# Tutorials

Interactive Jupyter notebook tutorials with complete code examples, visualizations, and detailed explanations.

## OHLCV Anomaly Detection

**Level**: Intermediate

**Topics**: Time series isolation forest anomaly detection, visualization, inspecting calculation pipeline

Learn how to detect anomalies in OHLCV (Open, High, Low, Close, Volume) financial data using chronos-lab's analysis pipeline. This tutorial covers:

- Fetching historical price data from multiple sources
- Running the anomaly detection algorithm
- Visualizing results with charts
- Saving results to datasets for later use
- Inspecting the calculation pipeline

[:material-notebook: Open Tutorial](ohlcv-anomaly-detection.ipynb){ .md-button .md-button--primary }

---

## Getting Started with Tutorials

### Prerequisites

Before running these tutorials, install chronos-lab with the required extras:

```bash
uv pip install chronos-lab[yfinance,arcticdb]
```

### Running Locally

1. Clone the repository:
   ```bash
   git clone https://github.com/vitaliknet/chronos-lab.git
   cd chronos-lab
   ```

2. Install dependencies:
   ```bash
   uv sync --all-extras
   ```

3. Launch Jupyter:
   ```bash
   jupyter notebook notebooks/
   ```

### Online Viewing

All tutorials are rendered with outputs (including charts) directly in the documentation. You can view them without installing anything!

---

## Contributing Tutorials

Have a great use case or analysis workflow? We'd love to see your tutorial!

**Guidelines:**
- Focus on real-world use cases
- Include clear explanations and comments
- Save notebooks with outputs (charts, tables)
- Add markdown cells for context
- Test that code runs end-to-end

Submit tutorials via [GitHub pull requests](https://github.com/vitaliknet/chronos-lab/pulls).
