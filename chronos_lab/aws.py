"""AWS integration utilities for SSM, Secrets Manager, S3, and DynamoDB.

This module provides utility functions and classes for interacting with AWS services
including Systems Manager Parameter Store, Secrets Manager, S3, DynamoDB, and Resource
Groups Tagging API. Requires AWS CLI configuration and boto3.

**IMPORTANT**: This is a utility module for AWS integrations. Most users won't need
to use these functions directly unless building custom AWS-integrated workflows.

Prerequisites:
    - AWS CLI configured (~/.aws/credentials)
    - boto3 installed (included with chronos-lab[aws] extra)
    - AWS_PROFILE environment variable set (if using named profiles)

Configuration:
    Set AWS profile via environment variable:
        export AWS_PROFILE=my-profile

    Or use default AWS credentials from ~/.aws/credentials

Typical Usage:
    Retrieve parameters from SSM Parameter Store:
        >>> from chronos_lab.aws import aws_get_parameters
        >>>
        >>> params = aws_get_parameters(
        ...     parameter_names=['/app/db/host', '/app/db/port'],
        ...     with_decryption=True
        ... )
        >>> print(params['/app/db/host'])

    Get secrets from Secrets Manager:
        >>> from chronos_lab.aws import aws_get_secret
        >>>
        >>> secret = aws_get_secret('my-database-credentials')
        >>> db_user = secret['username']
        >>> db_pass = secret['password']

    Work with DynamoDB:
        >>> from chronos_lab.aws import DynamoDBDatabase
        >>>
        >>> db = DynamoDBDatabase(table_name='my-table')
        >>> items = db.scan()
"""

from chronos_lab import logger
import boto3
from botocore.exceptions import ClientError
import time
import base64
import json
import os


def aws_get_parameters_by_path(parameter_path, with_decryption=False, recursive=False, format='dict'):
    """Retrieve all parameters from AWS Systems Manager Parameter Store by path.

    Fetches parameters from SSM Parameter Store using a hierarchical path with automatic
    pagination to retrieve all matching parameters.

    Args:
        parameter_path: SSM parameter path (e.g., '/app/database/' or '/prod/api/')
        with_decryption: If True, decrypt SecureString parameters. Defaults to False.
        recursive: If True, retrieve all parameters under the path hierarchy. Defaults to False.
        format: Return format - 'dict' returns {name: value} dict, any other value returns
            full API response. Defaults to 'dict'.

    Returns:
        If format='dict': Dictionary mapping parameter names to values
        Otherwise: Full API response from get_parameters_by_path
        Returns None on error.

    Examples:
        Get all database configuration parameters:
            >>> params = aws_get_parameters_by_path(
            ...     parameter_path='/app/database/',
            ...     with_decryption=True,
            ...     recursive=True
            ... )
            >>> db_host = params['/app/database/host']
            >>> db_port = params['/app/database/port']

    Note:
        - Automatically handles pagination for large parameter sets
        - Requires ssm:GetParametersByPath IAM permission
        - with_decryption requires kms:Decrypt permission for SecureString parameters
    """
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
    """Retrieve specific parameters from AWS Systems Manager Parameter Store.

    Fetches a list of named parameters from SSM Parameter Store.

    Args:
        parameter_names: List of parameter names to retrieve
        with_decryption: If True, decrypt SecureString parameters. Defaults to False.
        format: Return format - 'dict' returns {name: value} dict, any other value returns
            full API response. Defaults to 'dict'.

    Returns:
        If format='dict': Dictionary mapping parameter names to values
        Otherwise: Full API response from get_parameters
        Returns None on error.

    Examples:
        Get specific configuration parameters:
            >>> params = aws_get_parameters(
            ...     parameter_names=['/app/db/host', '/app/db/port', '/app/api/key'],
            ...     with_decryption=True
            ... )
            >>> db_host = params['/app/db/host']

    Note:
        - Maximum 10 parameters per call (AWS API limit)
        - Requires ssm:GetParameters IAM permission
    """
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
    """Retrieve a secret from AWS Secrets Manager.

    Fetches and decodes a secret value from AWS Secrets Manager. Automatically handles
    both string and binary secret types.

    Args:
        secret_name: Name or ARN of the secret to retrieve

    Returns:
        Parsed JSON dict if secret is a SecretString, or decoded binary if SecretBinary.
        Returns None on error.

    Examples:
        Get database credentials:
            >>> secret = aws_get_secret('prod/database/credentials')
            >>> db_user = secret['username']
            >>> db_password = secret['password']
            >>> db_host = secret['host']

        Get API key:
            >>> api_config = aws_get_secret('prod/api/config')
            >>> api_key = api_config['key']

    Note:
        - Secrets stored as JSON strings are automatically parsed
        - Requires secretsmanager:GetSecretValue IAM permission
        - Binary secrets are base64-decoded
    """
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
    """Parse an AWS ARN into its component parts.

    Breaks down an Amazon Resource Name (ARN) into structured components.

    Args:
        arn: AWS ARN string (e.g., 'arn:aws:s3:::my-bucket/key')

    Returns:
        Dictionary with keys:
            - arn: Original ARN
            - partition: AWS partition (usually 'aws')
            - service: AWS service name (e.g., 's3', 'dynamodb')
            - region: AWS region (e.g., 'us-east-1')
            - account: AWS account ID
            - resource: Resource identifier
            - resource_type: Resource type (extracted if present)
            - resource_name: Resource name (for secrets, extracts base name)

    Examples:
        Parse S3 bucket ARN:
            >>> arn_info = parse_arn('arn:aws:s3:::my-bucket/object.txt')
            >>> print(arn_info['service'])  # 's3'
            >>> print(arn_info['resource'])  # 'my-bucket/object.txt'

        Parse DynamoDB table ARN:
            >>> arn_info = parse_arn('arn:aws:dynamodb:us-east-1:123456789012:table/MyTable')
            >>> print(arn_info['resource_type'])  # 'table'
            >>> print(arn_info['resource'])  # 'MyTable'
    """
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
    """Retrieve all AWS resources with 'Name' tag using Resource Groups Tagging API.

    Fetches all tagged resources across your AWS account and parses their ARNs into
    structured information. Automatically handles pagination and filters out backup resources.

    Returns:
        Dictionary mapping resource names (from 'Name' tag) to parsed ARN dictionaries.
        Each ARN dict contains: arn, partition, service, region, account, resource,
        resource_type, resource_name.

    Examples:
        List all named resources:
            >>> resources = aws_get_resources()
            >>> for name, arn_info in resources.items():
            ...     print(f"{name}: {arn_info['service']} in {arn_info['region']}")

        Find specific resource:
            >>> resources = aws_get_resources()
            >>> my_db = resources.get('production-database')
            >>> if my_db:
            ...     print(f"Found: {my_db['arn']}")

    Note:
        - Automatically paginates through all resources
        - Requires tag:GetResources IAM permission
        - Filters resources by 'Name' tag
        - Excludes AWS Backup resources
    """
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
    """List objects in an S3 bucket with automatic pagination.

    Retrieves a list of objects from S3 bucket using the list_objects_v2 API with
    automatic pagination to fetch all matching objects.

    Args:
        **kwargs: Keyword arguments passed to s3.list_objects_v2(), including:
            - Bucket: S3 bucket name (required)
            - Prefix: Object key prefix filter (optional)
            - MaxKeys: Maximum objects per page (optional)
            - ContinuationToken: Pagination token (handled automatically)

    Returns:
        List of object dictionaries, each containing:
            - Key: Object key
            - LastModified: Last modification timestamp
            - Size: Object size in bytes
            - ETag: Entity tag
            - StorageClass: Storage class (e.g., 'STANDARD', 'GLACIER')
        Returns None on error.

    Examples:
        List all objects in a bucket:
            >>> objects = aws_s3_list_objects(Bucket='my-bucket')
            >>> for obj in objects:
            ...     print(f"{obj['Key']}: {obj['Size']} bytes")

        List objects with prefix:
            >>> objects = aws_s3_list_objects(
            ...     Bucket='my-bucket',
            ...     Prefix='data/2024/'
            ... )
            >>> print(f"Found {len(objects)} objects")

    Note:
        - Automatically paginates through all results
        - Requires s3:ListBucket IAM permission
        - Returns empty list if no objects match
    """
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
    """Wrapper class for AWS DynamoDB table operations.

    Provides convenient methods for common DynamoDB operations including put, update,
    batch operations, scan, and query with automatic pagination and error handling.

    Attributes:
        _ddb_client: boto3 DynamoDB resource
        _table: boto3 DynamoDB Table resource for the specified table

    Examples:
        Basic table operations:
            >>> db = DynamoDBDatabase(table_name='users')
            >>>
            >>> # Put item
            >>> db.put_item(Item={'user_id': '123', 'name': 'John'})
            >>>
            >>> # Scan table
            >>> all_items = db.scan()
            >>> print(f"Found {len(all_items)} items")

        Batch operations:
            >>> db = DynamoDBDatabase(table_name='products')
            >>>
            >>> items = [
            ...     {'product_id': '1', 'name': 'Widget', 'price': 9.99},
            ...     {'product_id': '2', 'name': 'Gadget', 'price': 19.99}
            ... ]
            >>> db.batch_write_items(Items=items)

    Note:
        - Requires appropriate DynamoDB IAM permissions
        - Table must exist before instantiation
        - Uses boto3 DynamoDB resource (not client)
    """
    def __init__(self, table_name: str):
        """Initialize DynamoDB table wrapper.

        Args:
            table_name: Name of the DynamoDB table
        """
        super().__init__()
        self._ddb_client = session.resource('dynamodb')
        self._table = self._ddb_client.Table(table_name)

    def put_item(self, **kwargs):
        """Put a single item into the DynamoDB table.

        Args:
            **kwargs: Arguments passed to table.put_item() including Item dict

        Returns:
            boto3 response dict on success, None on error

        Examples:
            >>> db = DynamoDBDatabase('users')
            >>> response = db.put_item(Item={'user_id': '123', 'name': 'John'})
        """
        try:
            response = self._table.put_item(**kwargs)
        except ClientError as error:
            logger.error(error.response['Error']['Message'])
            return None
        else:
            return response

    def update_item(self, **kwargs):
        """Update an existing item in the DynamoDB table.

        Args:
            **kwargs: Arguments passed to table.update_item() including Key,
                UpdateExpression, ExpressionAttributeValues, etc.

        Returns:
            boto3 response dict on success, None on error

        Examples:
            >>> db = DynamoDBDatabase('users')
            >>> response = db.update_item(
            ...     Key={'user_id': '123'},
            ...     UpdateExpression='SET #name = :name',
            ...     ExpressionAttributeNames={'#name': 'name'},
            ...     ExpressionAttributeValues={':name': 'Jane'}
            ... )
        """
        try:
            response = self._table.update_item(**kwargs)
        except ClientError as error:
            logger.error(error.response['Error']['Message'])
            return None
        else:
            return response

    def batch_write_items(self, Items):
        """Write multiple items to the table in batch.

        Uses batch_writer for efficient bulk writes with automatic retries.

        Args:
            Items: List of item dictionaries to write

        Returns:
            True on success, False on error

        Examples:
            >>> db = DynamoDBDatabase('products')
            >>> items = [
            ...     {'product_id': '1', 'name': 'Widget'},
            ...     {'product_id': '2', 'name': 'Gadget'}
            ... ]
            >>> success = db.batch_write_items(Items=items)
        """

        try:
            with self._table.batch_writer() as batch:
                for item in Items:
                    batch.put_item(Item=item)
            return True

        except Exception as error:
            logger.error("batch_writer() exception: %s", error)
            return False

    def batch_delete_items(self, Keys):
        """Delete multiple items from the table in batch.

        Uses batch_writer for efficient bulk deletes with automatic retries.

        Args:
            Keys: List of key dictionaries identifying items to delete

        Returns:
            True on success, False on error

        Examples:
            >>> db = DynamoDBDatabase('products')
            >>> keys = [
            ...     {'product_id': '1'},
            ...     {'product_id': '2'}
            ... ]
            >>> success = db.batch_delete_items(Keys=keys)
        """

        try:
            with self._table.batch_writer() as batch:
                for key in Keys:
                    batch.delete_item(Key=key)
            return True

        except Exception as error:
            logger.error("batch_writer() exception: %s", error)
            return False

    def batch_get_items(self, RequestItems, ReturnConsumedCapacity='NONE', tries=0, max_retries=5, init_wait=1):
        """Retrieve multiple items from one or more tables in batch with retry logic.

        Fetches items using batch_get_item with automatic retry and exponential backoff
        for unprocessed keys.

        Args:
            RequestItems: Dict mapping table names to item keys
            ReturnConsumedCapacity: Capacity consumption level ('NONE', 'TOTAL', 'INDEXES')
            tries: Current retry attempt (used internally)
            max_retries: Maximum retry attempts for unprocessed keys
            init_wait: Initial wait time in seconds for exponential backoff

        Returns:
            Dictionary mapping table names to lists of retrieved items

        Examples:
            >>> db = DynamoDBDatabase('users')
            >>> request = {
            ...     'users': {
            ...         'Keys': [
            ...             {'user_id': '123'},
            ...             {'user_id': '456'}
            ...         ]
            ...     }
            ... }
            >>> results = db.batch_get_items(RequestItems=request)
            >>> items = results['users']

        Note:
            - Automatically retries unprocessed keys with exponential backoff
            - Maximum wait time capped at 32 seconds
        """
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
        """Scan the entire table with automatic pagination.

        Performs a full table scan, automatically handling pagination to retrieve all items.

        Args:
            **kwargs: Arguments passed to table.scan() including FilterExpression,
                ProjectionExpression, etc.

        Returns:
            List of all items from the scan

        Examples:
            >>> db = DynamoDBDatabase('products')
            >>> all_items = db.scan()
            >>> print(f"Total items: {len(all_items)}")

            With filter:
            >>> from boto3.dynamodb.conditions import Attr
            >>> items = db.scan(FilterExpression=Attr('price').lt(100))

        Note:
            - Scans consume read capacity; use sparingly on large tables
            - Automatically paginates through all results
            - For targeted queries, use query() method instead
        """

        response = self._table.scan(**kwargs)
        result = response['Items']

        while 'LastEvaluatedKey' in response:
            kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = self._table.scan(**kwargs)
            result.extend(response['Items'])

        return result

    def query(self, **kwargs):
        """Query the table with automatic pagination.

        Queries items using partition key (and optionally sort key) with automatic
        pagination unless Limit is specified.

        Args:
            **kwargs: Arguments passed to table.query() including KeyConditionExpression,
                FilterExpression, IndexName, Limit, etc.

        Returns:
            If Limit specified: boto3 response dict with single page
            Otherwise: boto3 response dict with all paginated items in 'Items' key

        Examples:
            >>> from boto3.dynamodb.conditions import Key
            >>> db = DynamoDBDatabase('orders')
            >>>
            >>> # Query by partition key
            >>> response = db.query(
            ...     KeyConditionExpression=Key('customer_id').eq('123')
            ... )
            >>> orders = response['Items']
            >>>
            >>> # Query with sort key range
            >>> response = db.query(
            ...     KeyConditionExpression=(
            ...         Key('customer_id').eq('123') &
            ...         Key('order_date').between('2024-01-01', '2024-12-31')
            ...     )
            ... )

        Note:
            - Automatically paginates unless Limit is set
            - More efficient than scan for targeted queries
            - Requires partition key in KeyConditionExpression
        """
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
