# AWS Integration API

Low-level AWS utility functions for SSM Parameter Store, Secrets Manager, S3, DynamoDB, and Resource Groups Tagging API.

## Overview

The `chronos_lab.aws` module provides AWS integration utilities. These are low-level wrappers used internally by chronos-lab for AWS-based functionality.

!!! note "Low-Level API"
    Most users won't need to use these functions directly unless building custom AWS-integrated workflows. For dataset storage, use the high-level `to_dataset()` and `from_dataset()` functions instead.

**Prerequisites:**
- AWS CLI configured (`~/.aws/credentials`)
- boto3 installed (included with `chronos-lab[aws]` extra)
- Appropriate IAM permissions for the services used

## Functions

### Parameter Store

::: chronos_lab.aws.aws_get_parameters_by_path
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.aws.aws_get_parameters
    options:
      show_root_heading: true
      heading_level: 3

### Secrets Manager

::: chronos_lab.aws.aws_get_secret
    options:
      show_root_heading: true
      heading_level: 3

### Resource Management

::: chronos_lab.aws.parse_arn
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.aws.aws_get_resources
    options:
      show_root_heading: true
      heading_level: 3

### S3

::: chronos_lab.aws.aws_s3_list_objects
    options:
      show_root_heading: true
      heading_level: 3

## Classes

### DynamoDB

::: chronos_lab.aws.DynamoDBDatabase
    options:
      show_root_heading: true
      heading_level: 3
      members:
        - __init__
        - get_item
        - get_items
        - put_item
        - put_items
        - update_item
        - delete_item
