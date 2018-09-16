import boto3
import json
import datetime
import time
from ESLambdaLog import *
from LocalTime import *
from S3TextFromLambdaEvent import *

import os
import traceback
import logging
import structlog


def lambda_handler(event, context):
	log = structlog.get_logger()
	log.info("started", lamda_event=event)

	files_found = {}
	s3 = boto3.resource('s3')

	if "Records" not in event:
		return_message = get_return_message("Error: No key 'Records' in the event", files_found)
		log.error("invalid_event", return_message=return_message)
		return return_message

	file_refs = get_files_from_s3_lambda_event(event)
	file_text = get_file_text_from_s3_file_urls(file_refs, s3)

	return_message = get_return_message("Success", file_text)
	log.info("finished", return_message=return_message)
	return return_message


def get_index_from_path(path):
	return ""

def get_return_message(msg, files_found):
	return_message = {}
	return_message["msg"] = msg
	return_message["files_found"] = files_found
	return return_message
	



