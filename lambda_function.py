import boto3
from urllib.parse import *
import json
import datetime
import time
## from cache import *
from ESLambdaLog import *
from LocalTime import *
## from LinkCheckResult import *
## from Page import *
## from Link import *
## import sys
import os
import traceback
import logging
import structlog


def lambda_handler(event, context):

	log = structlog.get_logger()
	local_time = LocalTime()
	log.info("Starting ...")
	log.info("event_found", lamda_event=event)


	files_found = {}
	count = 0
	s3 = boto3.resource('s3')

	if "Records" not in event:
		return "Error: No key 'Records' in the event"

	for record in event["Records"]:
		count = count + 1
		newfile = record["s3"]["object"]["key"]
		bucket_arn = record["s3"]["bucket"]["arn"]
		bucket_name = get_bucket_name_from_arn(bucket_arn)
		file_url = get_bucket_file_url(record)
		print ("\t\tFile: " + file_url)
		files_found[count] = file_url

	log.info("Finished ...")
	return "Success"


def get_index_from_path(path):
	return ""



def get_bucket_file_url(record):
    #https://s3.amazonaws.com/link-checker/2018-05-27-235740.txt
    newfile = record["s3"]["object"]["key"]
    bucket_arn = record["s3"]["bucket"]["arn"]
    bucket_name = get_bucket_name_from_arn(bucket_arn)
    file_path = "https://s3.amazonaws.com/" + bucket_name + "/" + newfile
    return file_path

def get_bucket_name_from_arn(bucket_arn):
    bucket_name = bucket_arn.rsplit(":", 1)[-1]
    return bucket_name

