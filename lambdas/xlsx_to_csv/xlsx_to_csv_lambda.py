import boto3
import pandas as pd
import os
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get bucket and object key
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print(f"Processing file: s3://{bucket}/{key}")

    # Download file into memory
    obj = s3.get_object(Bucket=bucket, Key=key)
    file_content = obj['Body'].read()

    # Determine file extension
    _, extension = os.path.splitext(key)
    extension = extension.lower()

    if extension == ".xlsx":
        # Load Excel workbook
        excel_file = pd.ExcelFile(io.BytesIO(file_content))
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            # Build output key
            output_key = f"raw/orders/orders_{sheet_name}.csv"

            # Upload CSV to S3
            s3.put_object(
                Bucket=bucket,
                Key=output_key,
                Body=csv_buffer.getvalue()
            )
            print(f"Converted sheet '{sheet_name}' to {output_key}")

    elif extension == ".csv":
        # Simply copy CSV to raw zone
        output_key = f"raw/orders/{os.path.basename(key)}"
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': key},
            Key=output_key
        )
        print(f"Copied CSV to {output_key}")

    else:
        raise ValueError("Unsupported file type: " + extension)

    return {
        'statusCode': 200,
        'body': f"Processed file: {key}"
    }
