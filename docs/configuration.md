# Configuration

Complete guide to configuring chronos-lab for your environment.

## Configuration File

On first import, chronos-lab automatically creates `~/.chronos_lab/.env` from the bundled template. This file contains all configuration settings.

### Location

```
~/.chronos_lab/.env
```

### Default Contents

```bash
# Intrinio API Settings
# INTRINIO_API_KEY=

# ArcticDB Settings
ARCTICDB_LOCAL_PATH=~/.chronos_lab/arcticdb
ARCTICDB_DEFAULT_LIBRARY_NAME=uscomp
# ARCTICDB_S3_BUCKET=

# Logging
# LOG_LEVEL=DEBUG
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

## Configuration in Code

You can also access and use configuration programmatically:

```python
from chronos_lab.settings import get_settings

settings = get_settings()

print(f"Intrinio API Key: {settings.intrinio_api_key}")
print(f"ArcticDB Path: {settings.arcticdb_local_path}")
print(f"Default Library: {settings.arcticdb_default_library_name}")
print(f"Log Level: {settings.log_level}")
```

## Multiple Environments

### Development vs Production

Use different configuration files for different environments:

**Development** (`~/.chronos_lab/.env`):
```bash
ARCTICDB_LOCAL_PATH=~/dev/arcticdb
ARCTICDB_DEFAULT_LIBRARY_NAME=dev
LOG_LEVEL=DEBUG
```

**Production** (environment variables):
```bash
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
