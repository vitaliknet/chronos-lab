# Getting Started

## Installation

chronos-lab can be used in several ways, depending on how you want to engage with the framework—from interactive prototyping to library-style usage to extending its internals. The options below reflect the most common and effective workflows.

---

### 1. Explore with JupyterLab + tutorials

If you want to understand how chronos-lab is structured and become productive quickly, the best entry point is **JupyterLab** together with the published tutorials.

All notebooks in the **[Tutorials](/chronos-lab/tutorials/)** section have fully rendered output, including charts, making them the easiest way to start exploring.

This path lets you see unified data access, pre-built Hamilton DAGs, ArcticDB-backed storage, and integrated visualization in action, without committing to a full local development setup.

**Best for:** first-time users, research prototyping, interactive analysis

**Suggested steps:**

1. Start JupyterLab (locally, Google Colab, SageMaker, or similar)

2. Clone the GitHub repository

3. Work through the tutorial notebooks

```bash
git clone https://github.com/vitaliknet/chronos-lab.git
cd chronos-lab
```

This is the **recommended starting point** for most users.

---

### 2. Use chronos-lab as a Python library

If you already have an existing codebase and want to **embed chronos-lab into your own pipelines**, you can install it as a standard Python package.

This approach works well when you want to call chronos-lab DAGs, data access layers, or utilities from scripts, services, or notebooks that live outside this repository.

**Best for:** integration into existing projects, production pipelines, scheduled jobs

```bash
pip install chronos-lab
```

The base installation is intentionally minimal and includes only core dependencies. Most functionality is enabled via optional dependency groups:

```bash
pip install "chronos-lab[analysis,visualization,aws]"
```

Available extras include:

- `analysis` – feature engineering, modeling, and calculation graph utilities

- `arcticdb` – time-series storage with ArcticDB

- `aws` – AWS integrations (S3, DynamoDB)

- `ib` – Interactive Brokers market data (streaming and historical)

- `intrinio` – Intrinio market data

- `visualization` – matplotlib and mplfinance plotting

- `yfinance` – Yahoo Finance market data






Extras can be combined as needed to match your environment.

---

### 3. Develop, extend, or compose your own DAGs

If you want to **extend chronos-lab itself**, modify existing pipelines, or build your own Hamilton DAGs using chronos-lab functions as composable building blocks, a full development setup is recommended.

This option gives you direct access to internal DAG definitions, shared utilities, and execution patterns used throughout the project.

**Best for:** contributors, advanced users, custom DAG authors

```bash
git clone https://github.com/vitaliknet/chronos-lab.git
cd chronos-lab
```

Install dependencies using `uv`, including all optional extras:

```bash
uv sync --all-extras
```

This installs chronos-lab in editable mode and mirrors the environment used by maintainers, making it easier to prototype new DAGs or adapt existing ones.

---

### Python version support

chronos-lab requires **Python 3.12 or newer**. Python 3.13 is supported.

---

If you are unsure where to start, begin with **JupyterLab and the [tutorial notebooks](/chronos-lab/tutorials/)**, then move to a library or development setup as your needs become clearer.

