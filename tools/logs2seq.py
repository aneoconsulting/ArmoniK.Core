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

class LogSender:
    def __init__(self, url: str, max_batch_size: int = 100000):
        self.url = url
        self.max_batch_size = max_batch_size
        self.batch = b""
        self.ctr = 0


    def send_log(self, log_message: str):
        """
        Send a log message to a seq URL. logs are batched until the maximum batch size is reached (100000b),
        and then they are sent

        Args:
            log_message (str): The log message to send

        """
        if len(self.batch) + len(log_message) > self.max_batch_size:
            requests.post(self.url, data=self.batch)
            self.batch = b""
        self.batch += log_message
        self.ctr += 1
        logger.debug(f"sending : {len(self.batch)} bytes")


    def read_file(self, file_name: str):
        """
        Read the content of a text file, Gzip supported

        Args:
            file_name (str): The name of the file to read

        Returns:
            str: The content of the read file

        """
        if file_name.endswith('.json.tar.gz'):
            with gzip.open(file_name, 'rt', encoding="utf-8") as file:
                content = file.read()
        elif file_name.endswith('.json'):
            with open(file_name, 'r', encoding="utf-8") as file:
                content = file.read()
        return content


    def process_json_log(self, file_name: str):
        """
        Read a JSON log file, extract log messages, and send them to a Seq server.

        Args:
            file_name (str): The name of the file to read

        """
        json_data = self.read_file(file_name)
        for line in json_data.split('\n'):
            if line.startswith("{"):
                try:
                    parsed = json.loads(line)
                    if "@t" not in parsed:
                        continue
                    log_message = bytes(line + "\n", "utf-8")
                    self.send_log(log_message)
                except JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON: {e}")
        if self.batch != b"":
            self.send_log(self.batch)
            logger.info(self.batch)
        logger.info(f"sent: {self.ctr}")



def main():
    parser = argparse.ArgumentParser(description="Download ArmoniK logs in JSON CLEF format from S3 bucket then send them to Seq.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("bucket_name", help="S3 bucket", type=str)
    parser.add_argument("folder_name_core", help="Folder where core logs are located", type=str)
    parser.add_argument("run_number", help="GitHub workflow run_number", type=str)
    parser.add_argument("run_attempt", help="GitHub workflow run_attempt", type=str)
    parser.add_argument("file_name", help="file to download from the bucket", type=str)
    parser.add_argument("--url", dest="url", help="Seq url", type=str, default="http://localhost:9341/api/events/raw?clef")
    args = parser.parse_args()

    tmp_dir = "./tmp/"
    dir_name = os.path.join(
        args.folder_name_core, args.run_number, args.run_attempt)
    obj_name = os.path.join(dir_name, args.file_name)
    file_name = os.path.join(tmp_dir, obj_name)

    os.makedirs(os.path.join(tmp_dir, dir_name), exist_ok=True)

    s3 = boto3.client('s3')
    s3.download_file(args.bucket_name, obj_name, file_name)

    log_sender = LogSender(args.url)
    log_sender.process_json_log(file_name)

if __name__ == "__main__":
    main()