from chronos_lab import logger
import boto3
from botocore.exceptions import ClientError
import time
import base64
import json
import os


def aws_get_parameters_by_path(parameter_path, with_decryption=False, recursive=False, format='dict'):
    logger.info("Initiating aws:ssm client.")
    ssm_client = session.client('ssm')
    paginator = ssm_client.get_paginator('get_parameters_by_path')

    try:
        page_iterator = paginator.paginate(
            Path=parameter_path,
            Recursive=recursive,
            WithDecryption=with_decryption
        ).build_full_result()

    except ClientError as e:
        logger.error(e)
        return None

    if format == 'dict':
        parameters = {}

        for entry in page_iterator['Parameters']:
            parameters[entry['Name']] = entry['Value']
    else:
        return page_iterator

    logger.info("aws_get_parameters_by_path() is finishing successfully.")

    return parameters


def aws_get_parameters(parameter_names, with_decryption=False, format='dict'):
    logger.info("Initiating aws:ssm client.")
    ssm_client = session.client('ssm')

    try:
        response = ssm_client.get_parameters(Names=parameter_names,
                                             WithDecryption=with_decryption)
    except ClientError as e:
        logger.error(e)
        return None

    if format == 'dict':
        parameters = {}

        for entry in response['Parameters']:
            parameters[entry['Name']] = entry['Value']
    else:
        return response

    logger.info("aws_get_parameters() is finishing successfully.")

    return parameters


def aws_get_secret(secret_name):
    sm_client = session.client('secretsmanager')

    try:
        get_secret_value_response = sm_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        logger.error(e)
        return None
    else:
        if 'SecretString' in get_secret_value_response:
            return json.loads(get_secret_value_response['SecretString'])
        else:
            return base64.b64decode(get_secret_value_response['SecretBinary'])


def parse_arn(arn):
    elements = arn.split(':', 5)
    result = {
        'arn': arn,
        'partition': elements[1],
        'service': elements[2],
        'region': elements[3],
        'account': elements[4],
        'resource': elements[5],
        'resource_type': None,
        'resource_name': None
    }
    if '/' in result['resource']:
        result['resource_type'], result['resource'] = result['resource'].split('/', 1)
    elif ':' in result['resource']:
        result['resource_type'], result['resource'] = result['resource'].split(':', 1)

    if result['resource_type'] == 'secret':
        resource_parts = result['resource'].split('-', 2)
        result['resource_name'] = resource_parts[0] + '-' + resource_parts[1]
    return result


def aws_get_resources():
    from itertools import chain

    tagging = session.client('resourcegroupstaggingapi')

    paginator = tagging.get_paginator('get_resources')

    tag_mappings = chain.from_iterable(
        page['ResourceTagMappingList']
        for page in paginator.paginate(TagFilters=[{'Key': 'Name'}])
    )

    resources = {}

    for tag_mapping in tag_mappings:
        for tags in tag_mapping['Tags']:
            if tags['Key'] == 'Name':
                arn = parse_arn(tag_mapping['ResourceARN'])

                if arn['service'] != 'backup':
                    resources[tags['Value']] = arn

    return resources


def aws_s3_list_objects(**kwargs):
    s3_client = session.client('s3')

    paginator = s3_client.get_paginator('list_objects_v2')
    try:
        pages = paginator.paginate(**kwargs)
    except ClientError as e:
        logger.error(e)
        return None

    objects = []
    for page in pages:
        if page.get('Contents'):
            for obj in page['Contents']:
                objects.append(obj)

    return objects


class DynamoDBDatabase():
    def __init__(self, table_name: str):
        super().__init__()
        self._ddb_client = session.resource('dynamodb')
        self._table = self._ddb_client.Table(table_name)

    def put_item(self, **kwargs):
        try:
            response = self._table.put_item(**kwargs)
        except ClientError as error:
            logger.error(error.response['Error']['Message'])
            return None
        else:
            return response

    def update_item(self, **kwargs):
        try:
            response = self._table.update_item(**kwargs)
        except ClientError as error:
            logger.error(error.response['Error']['Message'])
            return None
        else:
            return response

    def batch_write_items(self, Items):

        try:
            with self._table.batch_writer() as batch:
                for item in Items:
                    batch.put_item(Item=item)
            return True

        except Exception as error:
            logger.error("batch_writer() exception: %s", error)
            return False

    def batch_delete_items(self, Keys):

        try:
            with self._table.batch_writer() as batch:
                for key in Keys:
                    batch.delete_item(Key=key)
            return True

        except Exception as error:
            logger.error("batch_writer() exception: %s", error)
            return False

    def batch_get_items(self, RequestItems, ReturnConsumedCapacity='NONE', tries=0, max_retries=5, init_wait=1):
        retrieved = {key: [] for key in RequestItems}

        while tries < max_retries:
            response = self._ddb_client.batch_get_item(RequestItems=RequestItems,
                                                       ReturnConsumedCapacity=ReturnConsumedCapacity)

            for key in response.get('Responses', []):
                retrieved[key] += response['Responses'][key]

            if ReturnConsumedCapacity != 'NONE':
                logger.info('Consumed Capacity: %s', response['ConsumedCapacity'])

            unprocessed = response['UnprocessedKeys']
            if len(unprocessed) > 0:
                batch_keys = unprocessed
                unprocessed_count = sum(
                    [len(batch_key['Keys']) for batch_key in batch_keys.values()])
                logger.info("%s unprocessed keys returned. Wait, then retry.", unprocessed_count)

                tries += 1
                if tries < max_retries:
                    logger.info("Sleeping for %s seconds.", sleepy_time)
                    time.sleep(sleepy_time)
                    sleepy_time = min(sleepy_time * 2, 32)
            else:
                break

        return retrieved

    def scan(self, **kwargs):

        response = self._table.scan(**kwargs)
        result = response['Items']

        while 'LastEvaluatedKey' in response:
            kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = self._table.scan(**kwargs)
            result.extend(response['Items'])

        return result

    def query(self, **kwargs):
        response = self._table.query(**kwargs)

        if kwargs.get('Limit'):
            return response

        result = response['Items']

        while 'LastEvaluatedKey' in response:
            logger.info('Starting next page with key %s', response['LastEvaluatedKey'])
            kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = self._table.query(**kwargs)
            result.extend(response['Items'])

        response['Items'] = result
        return response


logger.info("Setting up boto3 session.")

aws_profile = os.getenv('AWS_PROFILE')
session = boto3.Session(profile_name=aws_profile)
