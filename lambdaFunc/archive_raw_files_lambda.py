import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = "lakehouse-datastore"

    prefixes = [
        "raw/orders/",
        "raw/order_items/",
        "raw/products/"
    ]

    for prefix in prefixes:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        for page in pages:
            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                key = obj['Key']

                # Skip "folders"
                if key.endswith("/"):
                    continue

                # Destination key
                archive_key = key.replace("raw/", "archived/", 1)

                print(f"Archiving {key} -> {archive_key}")

                # Copy object
                s3.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': key},
                    Key=archive_key
                )

                # Delete original
                s3.delete_object(Bucket=bucket_name, Key=key)

    return {
        'statusCode': 200,
        'body': "Archiving completed."
    }
