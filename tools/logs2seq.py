import requests
import zipfile
import json
import io
import argparse
import boto3
import os
import gzip

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

def process_json_log(url, file_name):
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

def process_jsongz_log(url, file_name):
    batch = 0
    ctr = 0
    tosend = ""
    with gzip.open(file_name, "r") as file:
        for line in file.read().decode("utf-8").split("\n"):
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

if file_name.endswith(".json.tar.gz"):
    process_jsongz_log(args.url, file_name)
elif file_name.endswith(".json"):
    process_json_log(args.url, file_name)
