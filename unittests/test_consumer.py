import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import unittest
import json
import time
import boto3
from moto import mock_aws

from consumer import (
    fetch_request_s3,
    fetch_request_sqs,
    process_create_request,
    process_update_request,
    process_delete_request
)

REGION = "us-east-1"
REQUEST_BUCKET = "requests"
WIDGET_BUCKET = "widgets"
QUEUE_NAME = "widget-queue"


# ----------------------------------------------------
# S3 TESTS
# ----------------------------------------------------
@mock_aws
class TestConsumerS3(unittest.TestCase):

    def setUp(self):
        self.s3 = boto3.client("s3", region_name=REGION)
        self.s3.create_bucket(Bucket=REQUEST_BUCKET)
        self.s3.create_bucket(Bucket=WIDGET_BUCKET)

    def test_fetch_request_s3_empty(self):
        key, body = fetch_request_s3(self.s3, REQUEST_BUCKET)
        self.assertIsNone(key)
        self.assertIsNone(body)

    def test_fetch_request_single(self):
        self.s3.put_object(
            Bucket=REQUEST_BUCKET,
            Key="req1.json",
            Body=json.dumps({"type": "create", "widgetId": "1"})
        )
        key, body = fetch_request_s3(self.s3, REQUEST_BUCKET)
        self.assertEqual(key, "req1.json")
        data = json.loads(body)
        self.assertEqual(data["widgetId"], "1")

    def test_process_create_request_s3(self):
        data = {"type": "create", "widgetId": "22", "owner": "Alice", "label": "Test"}
        process_create_request(
            data,
            "bucket3",
            s3_client=self.s3,
            bucket_name=WIDGET_BUCKET
        )
        obj = self.s3.get_object(Bucket=WIDGET_BUCKET, Key="widgets/alice/22")
        stored = json.loads(obj["Body"].read())
        self.assertEqual(stored["widgetId"], "22")
        self.assertEqual(stored["owner"], "Alice")


# ----------------------------------------------------
# DynamoDB TESTS
# ----------------------------------------------------
@mock_aws
class TestConsumerDynamo(unittest.TestCase):

    def setUp(self):
        self.db = boto3.resource("dynamodb", region_name=REGION)
        self.db.create_table(
            TableName="widgets",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST"
        )
        self.table = self.db.Table("widgets")

    def test_process_create_request_dynamo(self):
        data = {
            "type": "create",
            "widgetId": "5",
            "owner": "Bob",
            "label": "Dynamo Test",
            "otherAttributes": [{"name": "color", "value": "red"}]
        }
        process_create_request(data, "dynamodb", dynamo_table=self.table)

        item = self.table.get_item(Key={"id": "5"})["Item"]
        self.assertEqual(item["owner"], "Bob")
        self.assertEqual(item["color"], "red")

    def test_process_update_request_dynamo(self):
        # Insert item
        self.table.put_item(Item={"id": "10", "owner": "Mark", "label": "Old"})

        update_data = {
            "type": "update",
            "widgetId": "10",
            "owner": "Mark",
            "label": "Updated",
            "description": "Desc",
            "otherAttributes": [{"name": "size", "value": "L"}]
        }

        process_update_request(update_data, "dynamodb", dynamo_table=self.table)

        item = self.table.get_item(Key={"id": "10"})["Item"]
        self.assertEqual(item["label"], "Updated")
        self.assertEqual(item["description"], "Desc")
        self.assertEqual(item["size"], "L")


# ----------------------------------------------------
# SQS TESTS
# ----------------------------------------------------
@mock_aws
class TestConsumerSQS(unittest.TestCase):

    def setUp(self):
        self.sqs = boto3.client("sqs", region_name=REGION)
        resp = self.sqs.create_queue(QueueName=QUEUE_NAME)
        self.queue_url = resp["QueueUrl"]

    # -------- TEST INPUT SQS FETCHER --------

    def test_fetch_request_sqs_empty(self):
        msg = fetch_request_sqs(self.sqs, self.queue_url)
        self.assertIsNone(msg)

    def test_fetch_request_sqs_with_messages(self):
        # Send 2 messages
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody="msg1")
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody="msg2")

        msg1 = fetch_request_sqs(self.sqs, self.queue_url)
        msg2 = fetch_request_sqs(self.sqs, self.queue_url)  # should come from cache

        self.assertEqual(msg1["Body"], "msg1")
        self.assertEqual(msg2["Body"], "msg2")

    # -------- TEST OUTPUT CREATE --------

    def test_process_create_request_sqs(self):
        data = {"type": "create", "widgetId": "100", "owner": "Ann"}
        process_create_request(
            data,
            "queue",
            queue_client=self.sqs,
            queue_url=self.queue_url
        )

        messages = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            MessageAttributeNames=["All"]
        )["Messages"]

        msg = messages[0]
        stored = json.loads(msg["Body"])
        self.assertEqual(stored["widgetId"], "100")
        self.assertEqual(msg["MessageAttributes"]["RequestType"]["StringValue"], "create")

    # -------- TEST OUTPUT UPDATE --------

    def test_process_update_request_sqs(self):
        data = {"type": "update", "widgetId": "200", "owner": "Jim"}

        process_update_request(
            data,
            "queue",
            sqs_client=self.sqs,
            queue_url=self.queue_url
        )

        messages = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            MessageAttributeNames=["All"]
        )["Messages"]

        msg = messages[0]
        body = json.loads(msg["Body"])

        self.assertEqual(body["event"], "WIDGET_UPDATED")
        self.assertEqual(body["widgetId"], "200")
        self.assertEqual(msg["MessageAttributes"]["UpdateType"]["StringValue"], "update")

    # -------- TEST OUTPUT DELETE --------

    def test_process_delete_request_sqs(self):
        data = {"type": "delete", "widgetId": "300", "owner": "Liz"}

        process_delete_request(
            data,
            "queue",
            sqs_client=self.sqs,
            queue_url=self.queue_url
        )

        messages = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            MessageAttributeNames=["All"]
        )["Messages"]

        msg = messages[0]
        body = json.loads(msg["Body"])

        self.assertEqual(body["event"], "WIDGET_DELETED")
        self.assertEqual(body["widgetId"], "300")
        self.assertEqual(msg["MessageAttributes"]["DeleteType"]["StringValue"], "delete")


# ----------------------------------------------------
# INVALID JSON TEST
# ----------------------------------------------------
@mock_aws
class TestInvalidJSON(unittest.TestCase):

    def setUp(self):
        self.s3 = boto3.client("s3", region_name=REGION)
        self.s3.create_bucket(Bucket=REQUEST_BUCKET)

    def test_invalid_json(self):
        self.s3.put_object(
            Bucket=REQUEST_BUCKET,
            Key="bad.json",
            Body='{"incomplete": "json"'
        )
        key, body = fetch_request_s3(self.s3, REQUEST_BUCKET)

        with self.assertRaises(json.JSONDecodeError):
            json.loads(body)


if __name__ == "__main__":
    unittest.main(verbosity=2)