# consumer.py
import argparse
import boto3
import logging
import time
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("consumer.log"),
        logging.StreamHandler()
    ]
)



def fetch_request(s3_client, bucket):
    response = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
    objects = response.get("Contents", [])
    if not objects:
        return None, None
    key = objects[0]["Key"]
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return key, body

def process_create_request(data, storage_type, s3_client=None, bucket_name=None, dynamo_table=None):
    # Convert otherAttributes array to dictionary
    other_attrs = {attr["name"]: attr["value"] for attr in data.get("otherAttributes", [])}

    if storage_type == "bucket3":
        key = f"widgets/{data['owner'].lower().replace(' ', '-')}/{data['widgetId']}"
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))
    elif storage_type == "dynamodb":
        item = {
            "id": data["widgetId"],
            "owner": data["owner"],
            "label": data.get("label", ""),
            "description": data.get("description", "")
        }
        item.update(other_attrs)
        dynamo_table.put_item(Item=item)
    else:
        raise ValueError("Invalid storage type")


def process_update_request(data, storage_type, s3_client=None, bucket_name=None, dynamo_table=None):
    other_attrs = {attr["name"]: attr["value"] for attr in data.get("otherAttributes", [])}

    if storage_type == "bucket3":
        key = f"widgets/{data['owner'].lower().replace(' ', '-')}/{data['widgetId']}"
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))
    elif storage_type == "dynamodb":
        update_expression = "SET #label = :label, #description = :description"
        expression_attribute_names = {"#label": "label", "#description": "description"}
        expression_attribute_values = {":label": data.get("label", ""), ":description": data.get("description", "")}

        for k, v in other_attrs.items():
            # Sanitize key to be a valid DynamoDB placeholder
            safe_key = k.replace("-", "_")
            update_expression += f", #{safe_key} = :{safe_key}"
            expression_attribute_names[f"#{safe_key}"] = k
            expression_attribute_values[f":{safe_key}"] = v

        dynamo_table.update_item(
            Key={"id": data["widgetId"]},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values
        )
    else:
        raise ValueError("Invalid storage type")



def process_delete_request(data, storage_type, s3_client=None, bucket_name=None, dynamo_table=None):
    if storage_type == "bucket3":
        key = f"widgets/{data['owner'].lower().replace(' ', '-')}/{data['widgetId']}"
        s3_client.delete_object(Bucket=bucket_name, Key=key)
    elif storage_type == "dynamodb":
        dynamo_table.delete_item(Key={"id": data["widgetId"]})
    else:
        raise ValueError("Invalid storage type")




def main():
    parser = argparse.ArgumentParser(description="Consumer program for Widget Requests")
    parser.add_argument("--storage", choices=["dynamodb", "bucket3"], required=True)
    parser.add_argument("--table")
    parser.add_argument("--bucket")
    parser.add_argument("--request-bucket", required=True)
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    # Setup storage clients
    if args.storage == "dynamodb":
        db = boto3.resource("dynamodb", region_name=args.region)
        table = db.Table(args.table)
        print(table.key_schema)

    else:
        s3 = boto3.client("s3", region_name=args.region)

    request_s3 = boto3.client("s3", region_name=args.region)

    while True:
        try:
            key, body = fetch_request(request_s3, args.request_bucket)
            if key is None:
                logging.info("No requests in %s; sleeping 100ms", args.request_bucket)
                time.sleep(0.1)
                continue

            logging.info("Processing request %s: %s", key, body)
            data = json.loads(body)

            # Decide which processing function to call
            client_args = {
                "s3_client": s3 if args.storage == "bucket3" else None,
                "bucket_name": args.bucket,
                "dynamo_table": table if args.storage == "dynamodb" else None
            }

            if data["type"] == "create":
                process_create_request(data, args.storage, **client_args)
            elif data["type"] == "delete":
                process_delete_request(data, args.storage, **client_args)
            elif data["type"] == "update":
                process_update_request(data, args.storage, **client_args)
            else:
                logging.warning("Unknown request type %s for key %s", data.get("type"), key)



            request_s3.delete_object(Bucket=args.request_bucket, Key=key)
            logging.info("Deleted request %s from request-bucket", key)

        except Exception as e:
            logging.error("Failed processing request %s: %s", key, e, exc_info=True)





if __name__ == "__main__":
    main()
