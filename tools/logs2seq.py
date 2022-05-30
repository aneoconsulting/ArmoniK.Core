import requests
import zipfile
import json
import io
import argparse
import boto3
import os

# How to run seq in docker
# docker rm -f seqlogpipe
# docker run -d --rm --name seqlogpipe -e ACCEPT_EULA=Y -p 9080:80 -p 9341:5341 datalust/seq

# Use the following command to configure AWS credentials
# aws configure

parser = argparse.ArgumentParser(description="Download ArmoniK logs in JSON CLEF format from S3 bucket then send them to Seq.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("bucket_name", help="S3 bucket", type=str)
parser.add_argument("run_number", help="GitHub workflow run_number", type=str)
parser.add_argument("run_attempt", help="GitHub workflow run_attempt", type=str)
parser.add_argument("file_name", help="file to download from the bucket", type=str)
parser.add_argument("--url", dest="url", help="Seq url", type=str, default="http://localhost:9341/api/events/raw?clef")
args = parser.parse_args()

dir_name = args.run_number + "/" + args.run_attempt + "/"
tmp_dir = "./tmp/"
obj_name = dir_name + args.file_name
file_name = tmp_dir + obj_name

os.makedirs(tmp_dir + dir_name, exist_ok=True)

s3 = boto3.client('s3')
s3.download_file(args.bucket_name, obj_name, file_name)

def process_log_file(url, content):
    batch = 0
    ctr = 0
    tosend = ""
    with open(file_name, "r") as file:
        for line in file.readlines():
            if line.startswith("{"):
                tosend += line + "\n"
            if batch > 100:
                requests.post(url, data=tosend)
                tosend = ""
                batch = 0
            batch = batch + 1
            ctr = ctr + 1
        print("sent :", ctr)
        if tosend != "":
            requests.post(url, data=tosend)


process_log_file(args.url, file_name)
