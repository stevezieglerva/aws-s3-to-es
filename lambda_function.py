import boto3
import json
import datetime
import time
from ESLambdaLog import *
from LocalTime import *
from S3TextFromLambdaEvent import *
import sys
import os
import traceback
import logging
import structlog


def lambda_handler(event, context):
	if "text_logging" in os.environ:
		log = structlog.get_logger()
	else:
		log = setup_logging()
	log = log.bind(lambda_name="aws-s3-to-es")
	log.critical("started", input_events=json.dumps(event, indent=3))

	files_found = {}
	s3 = boto3.resource('s3')

	if "Records" not in event:
		return_message = get_return_message("Error: No key 'Records' in the event", files_found)
		log.error("invalid_event", return_message=return_message)
		return return_message

	file_refs = get_files_from_s3_lambda_event(event)
	log.critical("got_file_refs", file_refs=file_refs)
	file_text = get_file_text_from_s3_file_urls(file_refs, s3)

	es = ESLambdaLog("ping_checks")
	for file in file_text:
		text = file_text[file]
		for document_line in text.splitlines():
			log.critical("prepping_to_index_in_ES", file=file, text=document_line)
			text_json = json.loads(document_line)
			es.log_event(text_json)

	return_message = get_return_message("Success", file_text)
	log.critical("finished", return_message=json.dumps(return_message, indent=3))
	return return_message


def get_index_from_path(path):
	return ""

def get_return_message(msg, files_found):
	return_message = {}
	return_message["msg"] = msg
	return_message["files_found"] = files_found
	return return_message
	
def setup_logging():
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO
    )
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger()


