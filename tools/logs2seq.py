import requests
import json
import argparse
import boto3
import os
import gzip
import logging
from pathlib import Path
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
    """
    LogSender is a class for sending log messages to a Seq server

    Args:
        url (str): The URL of the Seq server where log messages should be sent

    Attributes:
        url (str): The URL of the Seq server
        batch (bytes): A batch of log messages waiting to be sent
        ctr (int): A counter for the number of log messages sent

    Methods:
        sendlog(self, line: str):
            Send a log message to the Seq server. The message is expected to be in JSON format
            Logs are sent to the server when the batch size exceeds 100,000 bytes
            Logs left in the batch are sent at the exit
    """
    def __init__(self, url: str):
        self.url = url
        self.batch = b""
        self.ctr = 0

    def __enter__(self):
        return self

    def sendlog(self, line: str):
        """
        Send a log message to the Seq server

        Args:
            line (str): A log message in JSON format

        """
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
    """
    Process a JSON log file and send its contents to a Seq server

    Args:
        url (str): The URL of the Seq server where log messages should be sent
        file_name (str): The path to the JSON log file 

    Returns:
        None
    """
    with open(file_name, "r") as file:
        with LogSender(url) as log_sender:
            for line in file.readlines():
                log_sender.sendlog(line)
    

def process_jsongz_log(url: str, file_name: str):
    """
    Process a gzipped JSON log file and send its contents to a Seq server

    Args:
        url (str): The URL of the Seq server where log messages should be sent
        file_name (str): The path to the gzipped JSON log file

    Returns:
        None
    """
    with gzip.open(file_name, "r") as file:
        with LogSender(url) as log_sender:
            for line in file.read().decode("utf-8").split("\n"):
                log_sender.sendlog(line)




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

    if file_name.endswith(".json.tar.gz"):
        process_jsongz_log(args.url, file_name)
    elif file_name.endswith(".json"):
        process_json_log(args.url, file_name)

if __name__ == "__main__":
    main()