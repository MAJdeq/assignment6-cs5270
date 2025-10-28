# unittests/test_consumer.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import unittest
import json
import time
import boto3
from moto import mock_aws  # Moto v6+
from consumer import fetch_request, process_create_request

REGION = "us-east-1"
REQUEST_BUCKET = "requests"
WIDGET_BUCKET = "widgets"

# -----------------------
# S3 Tests
# -----------------------
@mock_aws
class TestConsumerS3(unittest.TestCase):
    def setUp(self):
        self.s3 = boto3.client("s3", region_name=REGION)
        self.s3.create_bucket(Bucket=REQUEST_BUCKET)
        self.s3.create_bucket(Bucket=WIDGET_BUCKET)

    def test_fetch_request_single_object(self):
        self.s3.put_object(Bucket=REQUEST_BUCKET, Key="req1.json",
                           Body='{"id":"1","requestType":"CREATE"}')
        key, body = fetch_request(self.s3, REQUEST_BUCKET)
        self.assertEqual(key, "req1.json")
        data = json.loads(body)
        self.assertEqual(data["id"], "1")

    def test_fetch_request_empty(self):
        key, body = fetch_request(self.s3, REQUEST_BUCKET)
        self.assertIsNone(key)
        self.assertIsNone(body)

    def test_process_create_request_s3(self):
        data = {"id": "123", "owner": "Alice", "label": "Test"}
        process_create_request(data, "bucket3", s3_client=self.s3, bucket_name=WIDGET_BUCKET)
        obj = self.s3.get_object(Bucket=WIDGET_BUCKET, Key="widgets/alice/123")
        body = json.loads(obj["Body"].read())
        self.assertEqual(body["id"], "123")
        self.assertEqual(body["owner"], "Alice")

# -----------------------
# DynamoDB Tests
# -----------------------
@mock_aws
class TestConsumerDynamoDB(unittest.TestCase):
    def setUp(self):
        self.db = boto3.resource("dynamodb", region_name=REGION)
        self.db.create_table(
            TableName="widgets",
            KeySchema=[{"AttributeName": "widget_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "widget_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST"
        )
        self.table = self.db.Table("widgets")

    def test_process_create_request_dynamodb(self):
        data = {"id": "123", "owner": "Bob", "label": "Test", "otherAttributes": {"color": "red"}}
        process_create_request(data, "dynamodb", dynamo_table=self.table)
        resp = self.table.get_item(Key={"widget_id": "123"})
        item = resp["Item"]
        self.assertEqual(item["owner"], "Bob")
        self.assertEqual(item["color"], "red")
        self.assertEqual(item["label"], "Test")

# -----------------------
# Invalid JSON Test
# -----------------------
@mock_aws
class TestInvalidJSON(unittest.TestCase):
    def setUp(self):
        self.s3 = boto3.client("s3", region_name=REGION)
        self.s3.create_bucket(Bucket=REQUEST_BUCKET)

    def test_invalid_json_request(self):
        self.s3.put_object(Bucket=REQUEST_BUCKET, Key="bad.json", Body='{"id":123')  # malformed JSON
        key, body = fetch_request(self.s3, REQUEST_BUCKET)
        with self.assertRaises(json.JSONDecodeError):
            json.loads(body)


if __name__ == "__main__":
    unittest.main(verbosity=2)
