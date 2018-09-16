import unittest
import time
from S3TextFromLambdaEvent import *
import boto3


event_empty = {}

event_one_file = {
	"Records": [
		{
		"eventVersion": "2.0",
		"eventTime": "1970-01-01T00:00:00.000Z",
		"requestParameters": {
			"sourceIPAddress": "127.0.0.1"
		},
		"s3": {
			"configurationId": "testConfigRule",
			"object": {
			"eTag": "0123456789abcdef0123456789abcdef",
			"sequencer": "0A1B2C3D4E5F678901",
			"key": "test_1.txt",
			"size": 1024
			},
			"bucket": {
			"arn": "arn:aws:s3:::aws-s3-to-es",
			"name": "sourcebucket",
			"ownerIdentity": {
				"principalId": "EXAMPLE"
			}
			},
			"s3SchemaVersion": "1.0"
		},
		"responseElements": {
			"x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
			"x-amz-request-id": "EXAMPLE123456789"
		},
		"awsRegion": "us-east-1",
		"eventName": "ObjectCreated:Put",
		"userIdentity": {
			"principalId": "EXAMPLE"
		},
		"eventSource": "aws:s3"
		}
	]
	}

event_two_files = {
	"Records": [
		{
		"eventVersion": "2.0",
		"eventTime": "1970-01-01T00:00:00.000Z",
		"requestParameters": {
			"sourceIPAddress": "127.0.0.1"
		},
		"s3": {
			"configurationId": "testConfigRule",
			"object": {
			"eTag": "0123456789abcdef0123456789abcdef",
			"sequencer": "0A1B2C3D4E5F678901",
			"key": "test_1.txt",
			"size": 1024
			},
			"bucket": {
			"arn": "arn:aws:s3:::aws-s3-to-es",
			"name": "sourcebucket",
			"ownerIdentity": {
				"principalId": "EXAMPLE"
			}
			},
			"s3SchemaVersion": "1.0"
		},
		"responseElements": {
			"x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
			"x-amz-request-id": "EXAMPLE123456789"
		},
		"awsRegion": "us-east-1",
		"eventName": "ObjectCreated:Put",
		"userIdentity": {
			"principalId": "EXAMPLE"
		},
		"eventSource": "aws:s3"
		},
		{
		"eventVersion": "2.0",
		"eventTime": "1970-01-01T00:00:00.000Z",
		"requestParameters": {
			"sourceIPAddress": "127.0.0.1"
		},
		"s3": {
			"configurationId": "testConfigRule",
			"object": {
			"eTag": "0123456789abcdef0123456789abcdef",
			"sequencer": "0A1B2C3D4E5F678901",
			"key": "test_2.txt",
			"size": 1024
			},
			"bucket": {
			"arn": "arn:aws:s3:::aws-s3-to-es",
			"name": "sourcebucket",
			"ownerIdentity": {
				"principalId": "EXAMPLE"
			}
			},
			"s3SchemaVersion": "1.0"
		},
		"responseElements": {
			"x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
			"x-amz-request-id": "EXAMPLE123456789"
		},
		"awsRegion": "us-east-1",
		"eventName": "ObjectCreated:Put",
		"userIdentity": {
			"principalId": "EXAMPLE"
		},
		"eventSource": "aws:s3"
		}		
	]
	}


class TestMethods(unittest.TestCase):
	def test_get_files_from_s3_lambda_event__empty_event__zero_files_returned(self):
		# Arrange

		# Act
		
		# Assert
		self.assertRaises(ValueError, get_files_from_s3_lambda_event, event_empty)

	def test_get_files_from_s3_lambda_event__one_file_in_event__one_file_returned(self):
		# Arrange

		# Act
		event = {}
		result = get_files_from_s3_lambda_event(event_one_file)
		
		# Assert
		self.assertEqual(len(result), 1)
		self.assertTrue("https://s3.amazonaws.com/aws-s3-to-es/test_1.txt" in result)
		self.assertEqual(result["https://s3.amazonaws.com/aws-s3-to-es/test_1.txt"]["bucket"], "aws-s3-to-es")
		self.assertEqual(result["https://s3.amazonaws.com/aws-s3-to-es/test_1.txt"]["key"], "test_1.txt")

	def test_get_files_from_s3_lambda_event__two_files_in_event__two_files_returned(self):
		# Arrange

		# Act
		event = {}
		result = get_files_from_s3_lambda_event(event_two_files)
		
		# Assert
		self.assertEqual(len(result), 2)
		self.assertTrue("https://s3.amazonaws.com/aws-s3-to-es/test_1.txt" in result)
		self.assertEqual(result["https://s3.amazonaws.com/aws-s3-to-es/test_1.txt"]["bucket"], "aws-s3-to-es")
		self.assertEqual(result["https://s3.amazonaws.com/aws-s3-to-es/test_1.txt"]["key"], "test_1.txt")

		self.assertTrue("https://s3.amazonaws.com/aws-s3-to-es/test_2.txt" in result)
		self.assertEqual(result["https://s3.amazonaws.com/aws-s3-to-es/test_2.txt"]["bucket"], "aws-s3-to-es")
		self.assertEqual(result["https://s3.amazonaws.com/aws-s3-to-es/test_2.txt"]["key"], "test_2.txt")


	def test_get_file_text_from_s3_file_urls__emtpy_dict__no_file_text_returned(self):
		# Arrange
		s3 = boto3.resource('s3')
		
		# Act
		result = get_file_text_from_s3_file_urls({}, s3)

		# Assert
		self.assertEqual(len(result), 0)




if __name__ == '__main__':
	unittest.main()		


