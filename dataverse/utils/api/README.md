# API
> This is a collection of API wrapper utilities for external sources

## 🥹 Use `original API` as much as you can
> **Recommend to use the `original API` as much as you can** rather than using this `wrapper`. **Because `original API` is universal and this `wrapper` is not.**

This is just for **ease usage of some external sources**. This is not a MUST to use. If you feel like you can do it yourself, we strongly recommend to do so.

Our purpose is to make a code easier to read and understand and normally `original API` is easy to read and understand for many people. This `wrapper` is just for some people who are not familiar with the `original API` or want to make a code more readable.


### ✅ Recommended (`original API`)
```python
import boto3

s3 = boto3.client("s3")
buckets = s3.list_buckets()['Buckets']
bucket_names = []
for bucket in buckets:
    bucket_names.append(bucket['Name'])
```

### ❌ Not Recommended (`wrapper`)
```python
from dataverse.utils.api import aws_s3_list_buckets

bucket_names = aws_s3_list_buckets()
```

## Support API
- aws

## Naming Convention
- `<api_name>_<function_name>`
    - e.g. `aws_s3_upload_file`
    - e.g. `aws_s3_download_file`