import requests
import zipfile
import json
import io
import argparse
import boto3
import os
import gzip
import logging
from pathlib import Path
from typing import IO
from json.decoder import JSONDecodeError


# How to run seq in docker
# docker rm -f seqlogpipe
# docker run -d --rm --name seqlogpipe -e ACCEPT_EULA=Y -p 9080:80 -p 9341:5341 datalust/seq

# Before using this script, you need to have a configured AWS sso profile.
# To create an AWS sso profile execute the following command:
#
# aws configure sso
#
# and follow the promp instructions, assuming you called your profile armonikDev you will need to execute
#
# aws sso login --profile=armonikDev
# to update your sso temporary credentials. Finally, you will be need to make your profile available to your aws cli
# by exporting the following enviromental variable:
#
# export AWS_PROFILE=armonikDev

logger = logging.getLogger(Path(__file__).name)
logging.basicConfig(
    level=logging.INFO
)

parser = argparse.ArgumentParser(description="Download ArmoniK logs in JSON CLEF format from S3 bucket then send them to Seq.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("bucket_name", help="S3 bucket", type=str)
parser.add_argument("folder_name_core", help="Folder where core logs are located", type=str)
parser.add_argument("run_number", help="GitHub workflow run_number", type=str)
parser.add_argument("run_attempt", help="GitHub workflow run_attempt", type=str)
parser.add_argument("file_name", help="file to download from the bucket", type=str)
parser.add_argument("--url", dest="url", help="Seq url", type=str, default="http://localhost:9341/api/events/raw?clef")
args = parser.parse_args()

dir_name = args.folder_name_core + "/" + args.run_number + "/" + args.run_attempt + "/"
tmp_dir = "./tmp/"
obj_name = dir_name + args.file_name
file_name = tmp_dir + obj_name

os.makedirs(tmp_dir + dir_name, exist_ok=True)

s3 = boto3.client('s3')
s3.download_file(args.bucket_name, obj_name, file_name)


class LogSender:
    def __init__(self, url: str):
        self.url = url
        self.batch = b""
        self.ctr = 0

    def __enter__(self):
        return self

    def sendlog(self, line: str):
        if line.startswith("{"):
            try:
                parsed = json.loads(line)
                if "@t" not in parsed:
                    return
                self.ctr = self.ctr + 1
                log_message = bytes(line + "\n", "utf-8")
                if len(self.batch) + len(log_message) > 100000:
                    requests.post(self.url, data=self.batch)
                    self.batch = log_message
                else:
                    self.batch += log_message
            except JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON: {e}")

    def __exit__(self, exception_type, exception_value, traceback):
        if self.batch != b"":
            requests.post(self.url, data=self.batch)
        logger.info(f"sent : {self.ctr}")
        

def process_json_log(url: str, file_name: str):
    with open(file_name, "r") as file:
        with LogSender(url) as log_sender:
            for line in file.readlines():
                log_sender.sendlog(line)
    


def process_jsongz_log(url: str, file_name: str):
    with gzip.open(file_name, "r") as file:
        with LogSender(url) as log_sender:
            for line in file.read().decode("utf-8").split("\n"):
                log_sender.sendlog(line)



if file_name.endswith(".json.tar.gz"):
    process_jsongz_log(args.url, file_name)
elif file_name.endswith(".json"):
    process_json_log(args.url, file_name)


