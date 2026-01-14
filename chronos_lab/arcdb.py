from chronos_lab import logger
from chronos_lab.settings import get_settings
import pandas as pd
import os
import arcticdb as adb
import concurrent.futures
from pathlib import Path


class ArcDB:
    def __init__(self,
                 *,
                 bucket_name=None,
                 local_path=None,
                 library_name):

        settings = get_settings()

        if not bucket_name:
            self._bucket_name = settings.arcticdb_s3_bucket
        else:
            self._bucket_name = bucket_name

        if not local_path:
            self._local_path = Path(settings.arcticdb_local_path).expanduser()
        else:
            self._local_path = Path(local_path).expanduser()

        self._library_name = library_name
        self._ac = None
        self._lib = None
        self._initialize_connection()

    def _initialize_connection(self):
        try:
            if self._bucket_name:
                import boto3

                logger.info("Setting up boto3 session.")

                aws_profile = os.getenv('AWS_PROFILE')
                session = boto3.Session(profile_name=aws_profile)

                uri = f"s3://s3.{session.region_name}.amazonaws.com:{self._bucket_name}?aws_auth=true"
                logger.info(f"Initializing ArcticDB with S3 backend using bucket: {uri}")
                self._ac = adb.Arctic(uri)
            elif self._local_path:
                logger.info(f"Initializing ArcticDB with local backend using path: {self._local_path}")
                if not os.path.exists(self._local_path):
                    os.makedirs(self._local_path)
                uri = f"lmdb://{self._local_path}"
                self._ac = adb.Arctic(uri)
            else:
                logger.warning(
                    "No storage backend specified. Using in-memory storage (not recommended for production).")
                self._ac = adb.Arctic("memory://")

            self._lib = self._ac.get_library(self._library_name, create_if_missing=True)
            logger.info(f"Successfully connected to ArcticDB library: {self._library_name}")

        except Exception as e:
            logger.error(f"Failed to initialize ArcticDB connection: {str(e)}")
            raise

    def batch_store(self,
                    data_dict,
                    mode='append',
                    **kwargs):

        response = {
            'statusCode': 0,
            'skipped_symbols': []
        }

        try:
            if not isinstance(data_dict, dict):
                logger.error("data_dict must be a dictionary")
                response['statusCode'] = -1
                return response

            payloads = []
            for symbol_key, data in data_dict.items():
                if not isinstance(data, pd.DataFrame):
                    logger.warning(f"Data for {symbol_key} is not a DataFrame, skipping")
                    response['skipped_symbols'].append(symbol_key)
                    continue

                payloads.append(adb.WritePayload(symbol_key, data))
                logger.debug(f"Created WritePayload for {symbol_key} with {len(data)} records")

            if not payloads:
                logger.warning("No valid data to write in batch")
                response['statusCode'] = -1
                return response

            if mode == 'append':
                results = self._lib.append_batch(payloads, **kwargs)
            else:
                results = self._lib.write_batch(payloads, **kwargs)

            for i, result in enumerate(results):
                symbol_key = payloads[i].symbol
                if hasattr(result, 'error_code') and result.error_code:
                    logger.error(f"Error writing data for {symbol_key}: {result}")
                    response['skipped_symbols'].append(symbol_key)
                else:
                    logger.info(f"Wrote data for {symbol_key}")

            if response['skipped_symbols']:
                response['statusCode'] = 1
                logger.warning(f"Failed to write {len(response['skipped_symbols'])} symbols")

            return response

        except Exception as e:
            logger.error(f"Error in batch write: {str(e)}")
            response['statusCode'] = -1
            return response

    def batch_update(self, data_dict, **kwargs):

        response = {
            'statusCode': 0,
            'skipped_symbols': []
        }

        try:
            if not isinstance(data_dict, dict):
                logger.error("data_dict must be a dictionary")
                response['statusCode'] = -1
                return response

            def update_symbol(symbol_key, data):
                try:
                    self._lib.update(symbol_key, data, **kwargs)
                    return symbol_key, True
                except Exception as e:
                    logger.error(f"Error updating data for {symbol_key}: {str(e)}")
                    return symbol_key, False

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(update_symbol, symbol_key, data): symbol_key
                           for symbol_key, data in data_dict.items()}

                for future in concurrent.futures.as_completed(futures):
                    symbol_key, success = future.result()
                    if not success:
                        response['skipped_symbols'].append(symbol_key)

            if response['skipped_symbols']:
                response['statusCode'] = 1
                logger.warning(f"Failed to update {len(response['skipped_symbols'])} symbols")

            return response

        except Exception as e:
            logger.error(f"Error in batch update: {str(e)}")
            response['statusCode'] = -1
            return response

    def batch_read(self, symbol_list, qb_join='inner', **kwargs):
        response = {
            'statusCode': 0,
            'payload': None
        }

        if not isinstance(symbol_list, list):
            logger.error("symbol_list must be a list")
            response['statusCode'] = -1
            return response

        if not symbol_list:
            logger.warning("Empty symbol_list provided")
            response['statusCode'] = -1
            return response

        read_requests = []
        for symbol_key in symbol_list:
            read_request = adb.ReadRequest(symbol_key, **kwargs)
            read_requests.append(read_request)
            logger.debug(f"Created ReadRequest for {symbol_key}")

        try:
            q = adb.QueryBuilder().concat(qb_join)
            df = self._lib.read_batch_and_join(read_requests, q).data
            logger.info(f"Successfully read {len(df)} total records across {len(symbol_list)} symbols")

            response['payload'] = df

        except Exception as e:
            logger.error(f"Error during read_batch_and_join: {str(e)}")
            response['statusCode'] = -1

        return response


