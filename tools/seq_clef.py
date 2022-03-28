import requests
import argparse

# How to run seq in docker
# docker run -d --rm --name seqlogpipe -e ACCEPT_EULA=Y -p 9080:80 -p 9341:5341 datalust/seq

parser = argparse.ArgumentParser(description="Read CLEF files and send them to Seq.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("file", help="path to the file", type=str)
parser.add_argument("--url", dest="url", help="Seq url", type=str, default="http://localhost:9341/api/events/raw?clef")
args = parser.parse_args()

def process_json_file(url, content):
    batch = 0
    ctr = 0
    tosend = ""
    for line in content.split("\n"):
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


with open(args.file) as f:
    process_json_file(args.url, f.read())
