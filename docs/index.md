# Welcome to Chronos Lab

chronos-lab is a batteries-included framework for financial time series analysis that turns best-in-class open-source tools into a single, coherent workflow.

It combines ArcticDB for time-series storage, Hamilton DAGs for transparent calculation pipelines, and scikit-learn for modeling—so you can ingest data, analyze thousands of symbols in parallel, and turn results into clear, inspectable insights with minimal glue code.

Connect directly to Interactive Brokers for real-time market data, or pull historical series from Yahoo Finance, Intrinio, and ArcticDB—all through a unified interface that delivers analysis-ready DataFrames.

Prototype interactively in Jupyter notebooks. Scale unchanged pipelines to production with AWS S3 and DynamoDB.

The goal isn’t novelty—it’s leverage. chronos-lab makes the tools you already trust work together, cleanly and predictably.

## Quick Links

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } __Getting Started__

    ---

    Install chronos-lab, run a workflow, explore common patterns

    [:octicons-arrow-right-24: Installation & Quick Start](getting-started.md)

-   :material-cog:{ .lg .middle } __Configuration__

    ---

    Configure API keys, storage backends, and environment settings

    [:octicons-arrow-right-24: Configuration Guide](configuration.md)

-   :material-book-open:{ .lg .middle } __API Reference__

    ---

    Complete documentation for all functions and classes

    [:octicons-arrow-right-24: Browse API Docs](api/index.md)

-   :material-notebook:{ .lg .middle } __Tutorials__

    ---

    Interactive Jupyter notebooks with visualizations and step-by-step guides

    [:octicons-arrow-right-24: Browse Tutorials](tutorials/index.md)

</div>

## Key Features

**Unified Market Data Access** : Pull OHLCV time series from Yahoo Finance, Intrinio, Interactive Brokers, or ArcticDB through a single, consistent interface — analysis-ready, UTC-normalized, and pandas-native from day one. Stream real-time tick and bar data from IB for live analysis workflows.

**Research-Grade Time Series Storage** : Store and retrieve large, versioned time series with ArcticDB, optimized for long histories, cross-sectional analysis, and rapid iteration across large universes.

**Pre-Built, Reusable Analysis** : Ready-to-use Hamilton DAGs cover common research workflows from ingestion to features, signals, and diagnostics. Use them as-is, adapt them to your research, or treat them as composable building blocks for new ideas.

**Parallel Multi-Symbol Processing** : Apply the same research logic across thousands of symbols efficiently, without hand-rolled batching or orchestration code.

**Notebook-to-Workflow Integration** : Run chronos-lab DAGs interactively in Jupyter, or embed them into larger workflows — from scheduled pipelines in Airflow to event-driven architectures on AWS.

**Opinionated, Modular Ecosystem** : Install only what you need via optional extras (yfinance, intrinio, arcticdb, aws). No reinvention — just tools designed to work together.


---

**Ready to dive in?** Start with the [Getting Started Guide](getting-started.md)
