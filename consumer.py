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

# -----------------------------------------------------
# S3 Request Fetcher
# -----------------------------------------------------
def fetch_request_s3(s3_client, bucket_name):
    resp = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
    contents = resp.get("Contents", [])
    if not contents:
        return None, None

    key = contents[0]["Key"]
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return key, body


# -----------------------------------------------------
# SQS Request Fetcher (with caching for up to 10 msgs)
# -----------------------------------------------------
_sqs_cache = []

def fetch_request_sqs(sqs_client, queue_url):
    global _sqs_cache

    # Return cached messages first
    if _sqs_cache:
        return _sqs_cache.pop(0)

    resp = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1,
        MessageAttributeNames=["All"]
    )
    messages = resp.get("Messages", [])
    if not messages:
        return None

    # Cache remaining messages
    _sqs_cache.extend(messages[1:])

    # Return first message
    return messages[0]


# -----------------------------------------------------
# Process Create Request
# -----------------------------------------------------
def process_create_request(data, storage_type, s3_client=None, bucket_name=None,
                           dynamo_table=None, queue_client=None, queue_url=None):

    other_attrs = {a["name"]: a["value"] for a in data.get("otherAttributes", [])}

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

    elif storage_type == "queue":
        queue_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(data),
            MessageAttributes={
                "RequestType": {
                    "DataType": "String",
                    "StringValue": "create"
                }
            }
        )
    else:
        raise ValueError("Invalid storage type")


# -----------------------------------------------------
# Process Update Request
# -----------------------------------------------------
def process_update_request(data, storage_type, s3_client=None, bucket_name=None,
                           dynamo_table=None, queue_client=None, queue_url=None):

    other_attrs = {a["name"]: a["value"] for a in data.get("otherAttributes", [])}

    if storage_type == "bucket3":
        key = f"widgets/{data['owner'].lower().replace(' ', '-')}/{data['widgetId']}"
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))

    elif storage_type == "dynamodb":
        update_expr = "SET #label = :label, #description = :desc"
        names = {"#label": "label", "#description": "description"}
        values = {
            ":label": data.get("label", ""),
            ":desc": data.get("description", "")
        }

        for k, v in other_attrs.items():
            safe = k.replace("-", "_")
            update_expr += f", #{safe} = :{safe}"
            names[f"#{safe}"] = k
            values[f":{safe}"] = v

        dynamo_table.update_item(
            Key={"id": data["widgetId"]},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values
        )

    elif storage_type == "queue":
        queue_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({
                "type": "update",
                "widgetId": data["widgetId"],
                "owner": data.get("owner"),
                "timestamp": int(time.time())
            }),
            MessageAttributes={
                "UpdateType": {
                    "DataType": "String",
                    "StringValue": "update"
                }
            }
        )

    else:
        raise ValueError("Invalid storage type")


# -----------------------------------------------------
# Process Delete Request
# -----------------------------------------------------
def process_delete_request(data, storage_type, s3_client=None, bucket_name=None,
                           dynamo_table=None, queue_client=None, queue_url=None):

    if storage_type == "bucket3":
        key = f"widgets/{data['owner'].lower().replace(' ', '-')}/{data['widgetId']}"
        s3_client.delete_object(Bucket=bucket_name, Key=key)

    elif storage_type == "dynamodb":
        dynamo_table.delete_item(Key={"id": data["widgetId"]})

    elif storage_type == "queue":
        queue_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({
                "type": "delete",
                "widgetId": data["widgetId"],
                "owner": data.get("owner"),
                "timestamp": int(time.time())
            }),
            MessageAttributes={
                "DeleteType": {
                    "DataType": "String",
                    "StringValue": "delete"
                }
            }
        )

    else:
        raise ValueError("Invalid storage type")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage", choices=["dynamodb", "bucket3", "queue"], required=True)
    parser.add_argument("--table")
    parser.add_argument("--bucket")
    parser.add_argument("--queue")               # queue name
    parser.add_argument("--request-bucket")      # request source (S3)
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    # ------------------- Backend storage clients -------------------
    storage_table = storage_s3 = storage_sqs = None

    if args.storage == "dynamodb":
        dynamodb = boto3.resource("dynamodb", region_name=args.region)
        storage_table = dynamodb.Table(args.table)
    elif args.storage == "bucket3":
        storage_s3 = boto3.client("s3", region_name=args.region)
    elif args.storage == "queue":
        storage_sqs = boto3.client("sqs", region_name=args.region)

    # ------------------- Request source: SQS -------------------
    request_sqs = request_queue_url = None
    if args.queue:
        request_sqs = boto3.client("sqs", region_name=args.region)
        # Get full URL from queue name
        response = request_sqs.get_queue_url(QueueName=args.queue)
        request_queue_url = response["QueueUrl"]

    # ------------------- Request source: S3 -------------------
    request_s3 = None
    if args.request_bucket:
        request_s3 = boto3.client("s3", region_name=args.region)

    while True:
        try:
            # ------------------- FETCH REQUEST -------------------
            if request_sqs:
                msg = fetch_request_sqs(request_sqs, request_queue_url)
                if not msg:
                    continue
                body = msg["Body"]
                receipt = msg["ReceiptHandle"]
                data = json.loads(body)
                key = None
            else:
                key, body = fetch_request_s3(request_s3, args.request_bucket)
                if key is None:
                    time.sleep(0.1)
                    continue
                data = json.loads(body)
                receipt = None

            logging.info(f"Processing: {data}")

            # ------------------- PROCESS REQUEST -------------------
            kwargs = {
                "s3_client": storage_s3 if args.storage == "bucket3" else None,
                "bucket_name": args.bucket,
                "dynamo_table": storage_table if args.storage == "dynamodb" else None,
                "queue_client": storage_sqs if args.storage == "queue" else None,
                "queue_url": request_queue_url
            }

            # Use "type" if available, otherwise fallback to "event"
            request_type = data.get("type") or data.get("event")
            match request_type:
                case "create":
                    process_create_request(data, args.storage, **kwargs)
                case "update" | "WIDGET_UPDATED":
                    process_update_request(data, args.storage, **kwargs)
                case "delete" | "WIDGET_DELETED":
                    process_delete_request(data, args.storage, **kwargs)
                case _:
                    logging.warning(f"Unknown request type: {data}")

            # ------------------- DELETE REQUEST -------------------
            if request_sqs and receipt:
                request_sqs.delete_message(
                    QueueUrl=request_queue_url,
                    ReceiptHandle=receipt
                )
            elif request_s3 and key:
                request_s3.delete_object(Bucket=args.request_bucket, Key=key)

        except Exception as e:
            logging.error(f"Error: {e}", exc_info=True)
 


if __name__ == "__main__":
    main()
