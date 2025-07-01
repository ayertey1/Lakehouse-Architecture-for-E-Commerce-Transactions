import re


def lambda_handler(event, context):
    process_orders = False
    process_order_items = False
    process_products = False

    keys = []

    # Case 1: Event from Step Function after xlsx_to_csv_lambda
    if "body" in event and isinstance(event["body"], str):
        pattern = r'Processed file: (.+)'
        match = re.search(pattern, event["body"])
        if match:
            keys.append(match.group(1).lower())

    # Case 2: S3 event
    elif "Records" in event:
        for record in event["Records"]:
            keys.append(record["s3"]["object"]["key"].lower())

    # Case 3: EventBridge event
    elif "detail" in event and "object" in event["detail"]:
        keys.append(event["detail"]["object"]["key"].lower())

    # Set flags
    for key in keys:
        if "orders" in key and "order_items" not in key:
            process_orders = True
        elif "order_items" in key:
            process_order_items = True
        elif "products" in key:
            process_products = True

    return {
        "process_orders": process_orders,
        "process_order_items": process_order_items,
        "process_products": process_products
    }
