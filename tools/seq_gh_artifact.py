import requests
import zipfile
import json
import io
import argparse

# How to run seq in docker
# docker run -d --rm --name seqlogpipe -e ACCEPT_EULA=Y -p 9080:80 -p 9341:5341 datalust/seq

parser = argparse.ArgumentParser(description="Download github artifact containing docker logs then send them to Seq while extracting the logs in CLEF format.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("token", help="GitHub TOKEN", type=str)
parser.add_argument("repository", help="GitHub repository ex : owner/repo", type=str)
parser.add_argument("run_id", help="GitHub workflow run_id", type=str)
parser.add_argument("artifact_name", help="artifact name (last characters)", type=str)
parser.add_argument("--url", dest="url", help="Seq url", type=str, default="http://localhost:9341/api/events/raw?clef")
args = parser.parse_args()

headers = {"Authorization" : "token " + args.token}

def process_log_file(url, content):
    batch = 0
    ctr = 0
    tosend = ""
    for line in content.decode("utf-8").split("\n"):
        if line.startswith("{"):
            jdata = json.loads(line)
            if jdata["log"].startswith("{"):
                tosend += jdata["log"] + "\n"
            else:
                print(jdata)
        if batch > 100:
            requests.post(url, data=tosend)
            tosend = ""
            batch = 0
        batch = batch + 1
        ctr = ctr + 1
    print("sent :", ctr)
    if tosend != "":
        requests.post(url, data=tosend)


def process_clef_file(url, content):
    batch = 0
    ctr = 0
    tosend = ""
    for line in content.decode("utf-8").split("\n"):
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


artifacts_str = requests.get(f"https://api.github.com/repos/{args.repository}/actions/runs/{args.run_id}/artifacts")
artifacts = json.loads(artifacts_str.content)["artifacts"]

for a in artifacts:
    if not a["name"].endswith(args.artifact_name) : continue
    print(a)
    mem_zip = requests.get(a['archive_download_url'], headers=headers)

    with zipfile.ZipFile(io.BytesIO(mem_zip.content)) as z:
        for file in z.namelist():
            if file.endswith(".log"):
                s = file.split("/")
                if s[1] in ["polling-agent", "worker-0", "control-plane"]:
                    print(file)
                    process_log_file(args.url, z.read(file))
            elif file.endswith(".json"):
                print(file)
                process_clef_file(args.url, z.read(file))
