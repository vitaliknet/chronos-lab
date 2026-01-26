# Configuration

## Configuration File

On first import, chronos-lab automatically creates `~/.chronos_lab/.env` from the bundled template. This file contains all configuration settings.

### Location

```
~/.chronos_lab/.env
```

### Default Contents

```bash
# Intrinio API Settings
#INTRINIO_API_KEY=

# Datatset Settings
DATASET_LOCAL_PATH=~/.chronos_lab/datasets
#DATASET_DDB_TABLE_NAME=
#DATASET_DDB_MAP='{
#    "ddb_watchlist": {
#        "pk": "map#ibpm#watchlist",
#        "sk": "name"
#    },
#    "ddb_securities_intrinio": {
#        "pk": "map#intrinio#securities"
#    },
#    "ddb_ohlcv_anomalies": {
#        "pk": "chronos_lab#ohlcv_anomalies"
#    }
#}'

# Store Settings
STORE_LOCAL_PATH=~/.chronos_lab/store
#STORE_S3_BUCKET=

# ArcticDB Settings
ARCTICDB_LOCAL_PATH=~/.chronos_lab/arcticdb
ARCTICDB_DEFAULT_LIBRARY_NAME=uscomp
#ARCTICDB_S3_BUCKET=

# Logging
#LOG_LEVEL=DEBUG
```

## Configuration Options

### Intrinio API

#### INTRINIO_API_KEY

Your Intrinio API key for accessing institutional financial data.

**Required for**: Using `ohlcv_from_intrinio()` or `securities_from_intrinio()`

**How to get**: Sign up at [intrinio.com](https://intrinio.com)

**Example**:
```bash
INTRINIO_API_KEY=your_api_key_here
```

### Dataset Storage

Datasets provide structured data storage for portfolio composition, watchlists, security metadata, and other non-time-series data. Datasets can be stored locally as JSON files or in AWS DynamoDB for distributed workflows.

**Important**: Datasets are for structured/metadata storage, NOT time series data. Use ArcticDB for OHLCV price data.

#### DATASET_LOCAL_PATH

Local filesystem path for dataset JSON file storage.

**Default**: `~/.chronos_lab/datasets`

**Supports**: Tilde expansion (`~`)

**Used by**: `to_dataset()` and `from_dataset()` for local storage

**Example**:
```bash
DATASET_LOCAL_PATH=~/data/datasets
```

#### DATASET_DDB_TABLE_NAME

AWS DynamoDB table name for dataset storage. Required for DynamoDB-backed datasets (names starting with `ddb_` prefix).

**Default**: None (DynamoDB disabled)

**Requires**:
- AWS CLI configuration (see [AWS DynamoDB Setup](#aws-dynamodb-setup) below)
- DATASET_DDB_MAP configuration

**Used by**: `to_dataset()` and `from_dataset()` for datasets with `ddb_` prefix

**Example**:
```bash
DATASET_DDB_TABLE_NAME=my-datasets-table
```

#### DATASET_DDB_MAP

JSON string mapping dataset names to DynamoDB key structure. Defines partition key (pk) and sort key (sk) patterns for each DynamoDB dataset.

**Default**: None

**Format**: JSON object with dataset names as keys, each containing:
- `pk`: Partition key pattern (required)
- `sk`: Sort key field name (optional, defaults to dataset item key)

**Example**:
```bash
DATASET_DDB_MAP='{
    "ddb_watchlist": {
        "pk": "map#ibpm#watchlist",
        "sk": "name"
    },
    "ddb_securities_intrinio": {
        "pk": "map#intrinio#securities"
    },
    "ddb_ohlcv_anomalies": {
        "pk": "chronos_lab#ohlcv_anomalies"
    }
}'
```

**Use Cases**:

- **Local datasets**: Portfolio composition, custom watchlists, backtesting configurations

- **DynamoDB datasets**: Distributed workflows where multiple processes share datasets


### File Storage

General-purpose file storage for plots, reports, and other binary content. Supports local filesystem and S3 backends.

**Important**: File storage is for arbitrary files (plots, PDFs, CSVs), NOT for time series data. Use ArcticDB for OHLCV price data and datasets for structured metadata.

#### STORE_LOCAL_PATH

Local filesystem path for general file storage.

**Default**: `~/.chronos_lab/store`

**Supports**: Tilde expansion (`~`)

**Used by**: `to_store()` for saving plots, charts, and other generated files locally

**Example**:
```bash
STORE_LOCAL_PATH=~/data/store
```

#### STORE_S3_BUCKET

AWS S3 bucket name for general file storage.

**Default**: None (S3 storage disabled)

**Requires**: AWS CLI configuration (see [AWS S3 Setup](#aws-s3-setup) below)

**Used by**: `to_store()` when `stores=['s3']` or `stores=['local', 's3']`

**Example**:
```bash
STORE_S3_BUCKET=my-charts-bucket
```

**Common Use Cases**:

- Saving analysis reports and visualizations

- Sharing generated content across distributed systems

### ArcticDB Storage

#### ARCTICDB_LOCAL_PATH

Local filesystem path for ArcticDB LMDB backend storage.

**Default**: `~/.chronos_lab/arcticdb`

**Supports**: Tilde expansion (`~`)

**Example**:
```bash
ARCTICDB_LOCAL_PATH=~/data/arctic
```

#### ARCTICDB_S3_BUCKET

AWS S3 bucket name for ArcticDB S3 backend storage.

**Takes precedence over**: `ARCTICDB_LOCAL_PATH`

**Requires**: AWS CLI configuration (see [AWS S3 Setup](#aws-s3-setup) below)

**Example**:
```bash
ARCTICDB_S3_BUCKET=my-timeseries-bucket
```

#### ARCTICDB_DEFAULT_LIBRARY_NAME

Default ArcticDB library name used when none is specified.

**Default**: `uscomp`

**Example**:
```bash
ARCTICDB_DEFAULT_LIBRARY_NAME=market_data
```

### Logging

#### LOG_LEVEL

Logging level for chronos-lab operations.

**Valid values**: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

**Default**: `INFO`

**Example**:
```bash
LOG_LEVEL=DEBUG
```

## Environment Variable Overrides

All settings can be overridden using environment variables. This is useful for:

- CI/CD environments
- Docker containers
- Temporary configuration changes

**Example**:
```bash
export INTRINIO_API_KEY="my_api_key"
export DATASET_DDB_TABLE_NAME="prod-datasets"
export ARCTICDB_DEFAULT_LIBRARY_NAME="production"
export LOG_LEVEL="WARNING"

python my_script.py
```

Environment variables take precedence over `.env` file settings.

## AWS S3 Setup

To use ArcticDB with AWS S3 backend:

### Step 1: Install Dependencies

```bash
# Install chronos-lab with arcticdb extra
uv pip install chronos-lab[arcticdb,aws]
```

### Step 2: Configure AWS CLI

```bash
# Install AWS CLI (if not already installed)
# macOS
brew install awscli

# Linux
pip install awscli

# Configure credentials
aws configure
```

You'll be prompted for:
- AWS Access Key ID
- AWS Secret Access Key
- Default region name
- Default output format

This creates `~/.aws/credentials` and `~/.aws/config`.

### Step 3: Set Environment Variables (Optional)

If using named AWS profiles:

```bash
export AWS_PROFILE=my-profile
```

### Step 4: Configure chronos-lab

Edit `~/.chronos_lab/.env`:

```bash
ARCTICDB_S3_BUCKET=my-timeseries-bucket
```

### Step 5: Verify

```python
from chronos_lab.arcdb import ArcDB

# This will use S3 backend
ac = ArcDB(library_name='test')
print("✓ S3 backend configured successfully")
```

## AWS DynamoDB Setup

To use datasets with AWS DynamoDB backend for distributed workflows:

### Step 1: Install Dependencies

```bash
# Install chronos-lab with aws extra
uv pip install chronos-lab[aws]
```

### Step 2: Configure AWS CLI

```bash
# Configure credentials (if not already done)
aws configure
```

This creates `~/.aws/credentials` and `~/.aws/config`.

### Step 3: Create DynamoDB Table

Use existing or create a table with partition key (pk) and sort key (sk):

```bash
aws dynamodb create-table \
    --table-name my-datasets-table \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=sk,AttributeType=S \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=sk,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST
```

### Step 4: Configure chronos-lab

Edit `~/.chronos_lab/.env`:

```bash
DATASET_DDB_TABLE_NAME=my-datasets-table
DATASET_DDB_MAP='{
    "ddb_securities": {
        "pk": "DATASET#securities",
        "sk": "ticker"
    },
    "ddb_portfolio": {
        "pk": "DATASET#portfolio",
        "sk": "symbol"
    }
}'
```

### Step 5: Verify

```python
from chronos_lab.storage import to_dataset
from chronos_lab.sources import from_dataset

# Write to DynamoDB
data = {
    'AAPL': {'name': 'Apple Inc.', 'sector': 'Technology'},
    'MSFT': {'name': 'Microsoft', 'sector': 'Technology'}
}
result = to_dataset(dataset_name='ddb_securities', dataset=data)

# Read from DynamoDB
securities = from_dataset(dataset_name='ddb_securities')
print(f"✓ DynamoDB backend configured successfully: {len(securities)} items")
```

**Distributed Workflow Example**:

One process writes datasets:
```python
# Process 1: Update security metadata daily
from chronos_lab.sources import securities_from_intrinio
from chronos_lab.storage import to_dataset

securities = securities_from_intrinio()
to_dataset(dataset_name="ddb_securities", dataset=securities.to_dict(orient='index'))
```

Other processes read datasets:
```python
# Process 2: Research workflow reads latest metadata
from chronos_lab.sources import from_dataset, ohlcv_from_arcticdb

securities = from_dataset(dataset_name='ddb_securities')
```

## Configuration in Code

You can also access and use configuration programmatically:

```python
from chronos_lab.settings import get_settings

settings = get_settings()

print(f"Intrinio API Key: {settings.intrinio_api_key}")
print(f"Dataset Local Path: {settings.dataset_local_path}")
print(f"Dataset DDB Table: {settings.dataset_ddb_table_name}")
print(f"Store Local Path: {settings.store_local_path}")
print(f"Store S3 Bucket: {settings.store_s3_bucket}")
print(f"ArcticDB Path: {settings.arcticdb_local_path}")
print(f"Default Library: {settings.arcticdb_default_library_name}")
print(f"Log Level: {settings.log_level}")
```

## Multiple Environments

### Development vs Production

Use different configuration files for different environments:

**Development** (`~/.chronos_lab/.env`):
```bash
DATASET_LOCAL_PATH=~/dev/datasets
STORE_LOCAL_PATH=~/dev/store
ARCTICDB_LOCAL_PATH=~/dev/arcticdb
ARCTICDB_DEFAULT_LIBRARY_NAME=dev
LOG_LEVEL=DEBUG
```

**Production** (environment variables):
```bash
export DATASET_DDB_TABLE_NAME=prod-datasets-table
export STORE_S3_BUCKET=prod-charts-bucket
export ARCTICDB_S3_BUCKET=prod-timeseries
export ARCTICDB_DEFAULT_LIBRARY_NAME=production
export LOG_LEVEL=WARNING
```

### Docker

For Docker containers, mount configuration or use environment variables:

**Option 1: Mount configuration file**
```dockerfile
docker run -v ~/.chronos_lab:/root/.chronos_lab my-image
```

**Option 2: Environment variables**
```dockerfile
ENV INTRINIO_API_KEY=your_key
ENV DATASET_DDB_TABLE_NAME=my-datasets-table
ENV STORE_S3_BUCKET=my-charts-bucket
ENV ARCTICDB_S3_BUCKET=my-bucket
ENV LOG_LEVEL=INFO
```

## Troubleshooting

### Configuration not loading

**Symptom**: Settings show None or defaults

**Solution**: Check file location and permissions:
```bash
ls -la ~/.chronos_lab/.env
cat ~/.chronos_lab/.env
```

### AWS S3 connection errors

**Symptom**: "Unable to locate credentials"

**Solution**: Verify AWS CLI configuration:
```bash
aws sts get-caller-identity
cat ~/.aws/credentials
```

### Intrinio API errors

**Symptom**: "Invalid API key"

**Solution**: Verify API key:
```bash
grep INTRINIO_API_KEY ~/.chronos_lab/.env
```

Make sure there are no extra spaces or quotes around the key.

## Best Practices

1. **Never commit `.env` files** - Add to `.gitignore`
2. **Use environment variables in CI/CD** - Don't store secrets in code
3. **Rotate API keys regularly** - Update in configuration file
4. **Use separate configurations per environment** - dev/staging/prod
5. **Monitor API usage** - Especially for paid services like Intrinio
6. **Back up S3 buckets** - Enable versioning and replication
