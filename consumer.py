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



if __name__ == "__main__":
    main()
