import boto3
import time

athena = boto3.client('athena')

def run_query(sql, database, output_bucket):
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output_bucket
        }
    )
    query_execution_id = response['QueryExecutionId']

    # Wait for completion
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(5)

    if state != 'SUCCEEDED':
        raise Exception(f"Query {query_execution_id} failed with state {state}")

    # Get results
    results = athena.get_query_results(QueryExecutionId=query_execution_id)
    row = results['ResultSet']['Rows'][1]
    count = int(row['Data'][0]['VarCharValue'])

    return count

def lambda_handler(event, context):
    database = 'lakehouse_datastore'
    output_bucket = 's3://lakehouse-datastore/athena-query-results/'

    orders_sql = 'SELECT COUNT(*) FROM lakehouse_orders'
    products_sql = 'SELECT COUNT(*) FROM lakehouse_products'
    order_items_sql = 'SELECT COUNT(*) FROM lakehouse_order_items'

    orders_count = run_query(orders_sql, database, output_bucket)
    products_count = run_query(products_sql, database, output_bucket)
    order_items_count = run_query(order_items_sql, database, output_bucket)

    return {
        'orders_count': orders_count,
        'products_count': products_count,
        'order_items_count': order_items_count
    }
