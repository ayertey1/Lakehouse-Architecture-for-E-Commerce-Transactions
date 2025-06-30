import boto3


def lambda_handler(event, context):
    glue = boto3.client('glue')
    crawler_name = "lakehouse-crawler"

    glue.start_crawler(Name=crawler_name)

    return {
        'statusCode': 200,
        'body': f"Crawler {crawler_name} started."
    }
