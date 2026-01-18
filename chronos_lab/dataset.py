from chronos_lab import logger
from chronos_lab.settings import get_settings
import pandas as pd
from pathlib import Path


class Dataset:
    def __init__(self,
                 *,
                 ddb_table_name=None,
                 local_path=None
                 ):

        settings = get_settings()

        if not ddb_table_name:
            self._table_name = settings.dataset_ddb_table_name
        else:
            self._table_name = ddb_table_name

        if not local_path:
            self._local_path = Path(settings.dataset_local_path).expanduser()
        else:
            self._local_path = Path(local_path).expanduser()

        if self._table_name:
            from chronos_lab.aws import DynamoDBDatabase
            self._database = DynamoDBDatabase(
                table_name=self._table_name)

        self._dataset_map = {}
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
