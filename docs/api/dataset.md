# Dataset Management API

Low-level dataset storage and retrieval for structured data (portfolios, watchlists, security metadata).

## Overview

The `chronos_lab.dataset` module manages structured datasets with dual storage backend support (local JSON files and DynamoDB).

!!! note "Low-Level API"
    Most users should use the high-level functions instead:

    - Use `from_dataset()` in `chronos_lab.sources` for reading datasets
    - Use `to_dataset()` in `chronos_lab.storage` for writing datasets

    Only use the `Dataset` class directly when building custom dataset management workflows.

**Dataset Naming Convention:**

- **Local datasets**: Use any name (stored as `{name}.json`)
- **DynamoDB datasets**: Prefix with `ddb_` (e.g., `ddb_securities`)

**Storage Backends:**

- **Local**: JSON files in `~/.chronos_lab/datasets` (configurable via `DATASET_LOCAL_PATH`)
- **DynamoDB**: AWS DynamoDB table (requires `DATASET_DDB_TABLE_NAME` configuration)

## Classes

::: chronos_lab.dataset.Dataset
    options:
      show_root_heading: true
      heading_level: 3
      members:
        - __init__
        - get_dataset
        - get_datasetDF
        - save_dataset
        - delete_dataset_items
