"""Dataset management for storing and retrieving structured data from local files or DynamoDB.

This module provides the Dataset class for managing structured datasets with support for
both local JSON file storage and AWS DynamoDB backend. Datasets can be retrieved as
dictionaries or pandas DataFrames with automatic type inference.

**IMPORTANT**: For most use cases, prefer the high-level functions:
    - Use from_dataset() in chronos_lab.sources for reading datasets
    - Use to_dataset() in chronos_lab.storage for writing datasets

Configuration:
    Set in ~/.chronos_lab/.env:
        DATASET_LOCAL_PATH=~/.chronos_lab/datasets
        DATASET_DDB_TABLE_NAME=my-datasets-table
        DATASET_DDB_MAP='{"ddb_dataset_name": {"pk": "DATASET#name", "sk": "id"}}'

Typical Usage:
    Read dataset as DataFrame:
        >>> from chronos_lab.sources import from_dataset
        >>>
        >>> # From local file
        >>> df = from_dataset(dataset_name='example')
        >>>
        >>> # From DynamoDB
        >>> df = from_dataset(dataset_name='ddb_my_data')

    Write dataset:
        >>> from chronos_lab.storage import to_dataset
        >>>
        >>> data = {'item1': {'value': 100}, 'item2': {'value': 200}}
        >>> result = to_dataset(dataset_name='my_data', dataset=data)

Dataset Naming Convention:
    - Local datasets: Use any name (stored as {name}.json)
    - DynamoDB datasets: Prefix with 'ddb_' (e.g., 'ddb_securities')

Note:
    - Local datasets are stored as JSON files
    - DynamoDB datasets require table configuration in settings
    - DataFrames automatically infer datetime and numeric types
"""

from chronos_lab import logger
from chronos_lab.settings import get_settings
import pandas as pd
from pathlib import Path


class Dataset:
    """Manager for structured datasets stored locally or in DynamoDB.

    Handles reading and writing datasets with support for both local JSON files and
    AWS DynamoDB tables. Automatically manages dataset locations based on naming
    conventions and configuration.

    Attributes:
        _table_name: DynamoDB table name (if configured)
        _local_path: Local filesystem path for JSON datasets
        _dataset_map: Mapping of dataset names to DynamoDB keys (pk/sk)
        _database: DynamoDBDatabase instance (if DynamoDB configured)

    Examples:
        Work with local datasets:
            >>> ds = Dataset()
            >>> # Get as dictionary
            >>> data_dict = ds.get_dataset(dataset_name='example')
            >>> # Get as DataFrame
            >>> df = ds.get_datasetDF(dataset_name='example')

        Work with DynamoDB datasets:
            >>> ds = Dataset(ddb_table_name='my-datasets')
            >>> data = ds.get_dataset(dataset_name='ddb_securities')
            >>> df = ds.get_datasetDF(dataset_name='ddb_securities')

    Note:
        - Local datasets: Names without 'ddb_' prefix
        - DynamoDB datasets: Names with 'ddb_' prefix
        - DynamoDB requires DATASET_DDB_TABLE_NAME and DATASET_DDB_MAP in settings
    """
    def __init__(self,
                 *,
                 ddb_table_name=None,
                 local_path=None
                 ):
        """Initialize Dataset manager with local and/or DynamoDB configuration.

        Args:
            ddb_table_name: DynamoDB table name. If None, uses DATASET_DDB_TABLE_NAME
                from configuration.
            local_path: Local filesystem path for JSON datasets. If None, uses
                DATASET_LOCAL_PATH from configuration.
        """

        settings = get_settings()

        if not ddb_table_name:
            self._table_name = settings.dataset_ddb_table_name
        else:
            self._table_name = ddb_table_name

        if not local_path:
            self._local_path = Path(settings.dataset_local_path).expanduser()
        else:
            self._local_path = Path(local_path).expanduser()

        self._dataset_map = {}
        if self._table_name:
            from chronos_lab.aws import DynamoDBDatabase
            self._database = DynamoDBDatabase(
                table_name=self._table_name)

            if settings.dataset_ddb_map:
                import json
                try:
                    self._dataset_map = json.loads(settings.dataset_ddb_map)
                    logger.info("Loaded DynamoDB dataset map from settings")
                except Exception as e:
                    logger.error("Failed to load dataset map from settings: %s", e)

    def get_dataset(self,
                    *,
                    dataset_name):
        """Retrieve a dataset as a dictionary.

        Fetches dataset from local JSON file or DynamoDB table based on naming convention.

        Args:
            dataset_name: Dataset identifier. Use 'ddb_' prefix for DynamoDB datasets,
                no prefix for local JSON files.

        Returns:
            Dictionary with keys:
                - 'statusCode': 0 on success, -1 on failure
                - 'payload': Dictionary of dataset items (keys to attribute dicts)

        Examples:
            Get local dataset:
                >>> ds = Dataset()
                >>> result = ds.get_dataset(dataset_name='example')
                >>> if result['statusCode'] == 0:
                ...     data = result['payload']
                ...     print(data.keys())

            Get DynamoDB dataset:
                >>> ds = Dataset()
                >>> result = ds.get_dataset(dataset_name='ddb_securities')
                >>> data = result['payload']

        Note:
            - Local datasets loaded from {DATASET_LOCAL_PATH}/{name}.json
            - DynamoDB datasets require configuration in DATASET_DDB_MAP
            - DynamoDB items are keyed by their 'sk' (sort key) value
        """

        ret = {
            'statusCode': -1,
            'payload': None
        }

        if not dataset_name.startswith('ddb_'):
            json_file = self._local_path / f"{dataset_name}.json"
            if json_file.exists():
                import json
                with open(json_file, 'r') as f:
                    ret['payload'] = json.load(f)
                    ret['statusCode'] = 0
            else:
                logger.warning('Local dataset file not found: %s', json_file)
            return ret
        elif not self._table_name:
            logger.warning('DATASET_DDB_TABLE_NAME is not defined, will not query DynamoDB.')
            return ret

        from boto3.dynamodb.conditions import Key

        if self._dataset_map.get(dataset_name):
            ds_keys = self._dataset_map[dataset_name]
        else:
            logger.error('Dataset is not defined.')
            return ret

        response_db = self._database.query(
            KeyConditionExpression=Key('pk').eq(ds_keys['pk'])
        )

        if response_db['Count'] > 0:
            ds_data = {}
            for item in response_db['Items']:
                ds_data_key = item['sk']

                del item['pk']
                del item['sk']
                ds_data[ds_data_key] = item

            ret['payload'] = ds_data
            ret['statusCode'] = 0

            return ret
        else:
            logger.warning('No items are returned for dataset %s', dataset_name)
            return ret

    def get_datasetDF(self,
                      **kwargs):
        """Retrieve a dataset as a pandas DataFrame with automatic type inference.

        Fetches dataset and converts to DataFrame with automatic detection and conversion
        of datetime and numeric columns.

        Args:
            **kwargs: Arguments passed to get_dataset(), including dataset_name

        Returns:
            pandas DataFrame with inferred types, or None on error

        Examples:
            Get local dataset as DataFrame:
                >>> ds = Dataset()
                >>> df = ds.get_datasetDF(dataset_name='example')
                >>> print(df.head())
                >>> print(df.dtypes)

            Get DynamoDB dataset as DataFrame:
                >>> ds = Dataset()
                >>> df = ds.get_datasetDF(dataset_name='ddb_securities')
                >>> # DataFrame index is the sort key (sk) from DynamoDB

        Note:
            - Automatically converts ISO datetime strings to pandas datetime
            - Automatically converts numeric strings to numeric types
            - Index is the dataset keys (filename for local, 'sk' for DynamoDB)
        """
        sa_ret = self.get_dataset(**kwargs)

        if sa_ret['statusCode'] == 0:
            result = pd.DataFrame.from_dict(sa_ret['payload'], orient='index')

            mask_dt = result.astype(str).apply(
                lambda x: x.str.match(r'\d{4}-\d{2}-\d{2}T\d{2}\:\d{2}\:\d{2}\.\d{3}Z').any())
            result.loc[:, mask_dt] = result.loc[:, mask_dt].apply(pd.to_datetime, errors='coerce')

            mask_num = result.astype(str).apply(lambda x: x.str.match(r'^[-]?\d*[.]?\d*$').any())
            result.loc[:, mask_num] = result.loc[:, mask_num].apply(pd.to_numeric, errors='coerce')

            return result.infer_objects()
        else:
            return None

    def save_dataset(self,
                     dataset_name,
                     dataset):
        """Save a dataset to local JSON file or DynamoDB table.

        Stores dataset dictionary based on naming convention. Creates parent directories
        if needed for local storage.

        Args:
            dataset_name: Dataset identifier. Use 'ddb_' prefix for DynamoDB,
                no prefix for local JSON.
            dataset: Dictionary of items to save (keys to attribute dicts)

        Returns:
            Dictionary with 'statusCode': 0 on success, -1 on failure

        Examples:
            Save to local JSON:
                >>> ds = Dataset()
                >>> data = {
                ...     'item1': {'name': 'Product A', 'price': 9.99},
                ...     'item2': {'name': 'Product B', 'price': 19.99}
                ... }
                >>> result = ds.save_dataset('products', data)

            Save to DynamoDB:
                >>> ds = Dataset()
                >>> data = {
                ...     'AAPL': {'name': 'Apple Inc.', 'sector': 'Technology'},
                ...     'MSFT': {'name': 'Microsoft', 'sector': 'Technology'}
                ... }
                >>> result = ds.save_dataset('ddb_securities', data)

        Note:
            - Local datasets saved to {DATASET_LOCAL_PATH}/{name}.json
            - DynamoDB datasets require configuration in DATASET_DDB_MAP
            - DynamoDB items get 'pk' and 'sk' added automatically from map
            - JSON dates are serialized as strings using default=str
        """

        if not dataset_name.startswith('ddb_'):
            import json
            json_file = self._local_path / f"{dataset_name}.json"
            try:
                self._local_path.mkdir(parents=True, exist_ok=True)
                with open(json_file, 'w') as f:
                    json.dump(dataset, f, indent=2, default=str)
                return {'statusCode': 0}
            except Exception as e:
                logger.error('Failed to save local dataset: %s', e)
                return {'statusCode': -1}
        elif not self._table_name:
            logger.warning('DATASET_DDB_TABLE_NAME is not defined, will not save to DynamoDB.')
            return {'statusCode': -1}

        if self._dataset_map.get(dataset_name):
            ds_keys = self._dataset_map[dataset_name]
        else:
            logger.error('Dataset is not defined')
            return {'statusCode': -1}

        items = []

        for (key, item) in dataset.items():
            item['pk'] = ds_keys['pk']

            if ds_keys.get('sk'):
                item['sk'] = item[ds_keys['sk']]
            else:
                item['sk'] = key
            items.append(item)

        if not self._database.batch_write_items(Items=items):
            logger.error('Failed to save items')
            return {'statusCode': -1}
        else:
            return {'statusCode': 0}

    def delete_dataset_items(self,
                             dataset_name,
                             items):
        """Delete specific items from a DynamoDB dataset.

        Removes items from DynamoDB table using batch delete. Not supported for local
        JSON datasets.

        Args:
            dataset_name: DynamoDB dataset name (must start with 'ddb_')
            items: List of sort key (sk) values identifying items to delete

        Returns:
            Dictionary with 'statusCode': 0 on success, -1 on failure

        Examples:
            Delete items from DynamoDB dataset:
                >>> ds = Dataset()
                >>> items_to_delete = ['AAPL', 'MSFT', 'GOOGL']
                >>> result = ds.delete_dataset_items(
                ...     dataset_name='ddb_securities',
                ...     items=items_to_delete
                ... )
                >>> if result['statusCode'] == 0:
                ...     print("Items deleted successfully")

        Note:
            - Only works with DynamoDB datasets (names starting with 'ddb_')
            - Not supported for local JSON datasets
            - Requires DATASET_DDB_TABLE_NAME configuration
            - Items identified by their sort key (sk) values
            - Uses batch delete for efficiency
        """
        if not self._table_name:
            logger.warning('DATASET_DDB_TABLE_NAME is not defined, will not delete items from DynamoDB.')
            return {'statusCode': -1}

        if not dataset_name.startswith('ddb_'):
            logger.warning('Items deletes from a local dataset is not supported.')
            return {'statusCode': -1}

        if self._dataset_map.get(dataset_name):
            ds_keys = self._dataset_map[dataset_name]
        else:
            logger.error('Dataset is not defined')
            return {'statusCode': -1}

        if not isinstance(items, list):
            logger.error('items argument must be a list of sk')
            return {'statusCode': -1}

        items_dict = []
        for item_sk in items:
            items_dict.append(
                {
                    'pk': ds_keys['pk'],
                    'sk': item_sk
                }
            )

        if not self._database.batch_delete_items(Keys=items_dict):
            logger.error('Failed to delete items')
            return {'statusCode': -1}
        else:
            return {'statusCode': 0}


def _init_local_path():
    try:
        settings = get_settings()
        if not settings.dataset_local_path:
            logger.error(
                "Could not initialize local dataset storage directory and example: DATASET_LOCAL_PATH is not defined in settings")
            raise Exception("DATASET_LOCAL_PATH is not defined in settings")

        local_path = Path(settings.dataset_local_path).expanduser()

        example_file = local_path / "example.json"

        if not example_file.exists():
            import shutil
            import sys

            local_path.mkdir(parents=True, exist_ok=True)

            package_dir = Path(__file__).parent
            json_example = package_dir / ".dataset.example.json"

            if json_example.exists():
                shutil.copy(json_example, example_file)
                logger.info(f"✓ Chronos Lab: Created local dataset storage directory and example at {example_file}")

    except Exception as e:
        logger.error(f"Could not create local dataset storage directory and example: {e}")


_init_local_path()

del _init_local_path
