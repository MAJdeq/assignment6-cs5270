# tests/test_s3_with_moto.py
import unittest
import boto3
from moto import mock_aws
from aws_s3 import upload_text, download_text, list_keys

BUCKET = "demo-bucket"
REGION = "us-east-1"

@mock_aws
class TestS3WithMoto(unittest.TestCase):
    def setUp(self):
        self.s3 = boto3.client("s3", region_name=REGION)
        self.s3.create_bucket(Bucket=BUCKET)

    def test_upload_and_download_text(self):
        upload_text(BUCKET, "folder/hello.txt", "hello moto", s3=self.s3)
        out = download_text(BUCKET, "folder/hello.txt", s3=self.s3)
        self.assertEqual(out, "hello moto")

    def test_list_keys_with_prefix(self):
        for i in range(3):
            upload_text(BUCKET, f"data/file_{i}.txt", f"payload {i}", s3=self.s3)
        keys = list_keys(BUCKET, prefix="data/", s3=self.s3)
        self.assertEqual(sorted(keys), ["data/file_0.txt", "data/file_1.txt", "data/file_2.txt"])

    def test_download_missing_raises(self):
        with self.assertRaises(FileNotFoundError):
            download_text(BUCKET, "missing.txt", s3=self.s3)

if __name__ == "__main__":
    unittest.main(verbosity=2)