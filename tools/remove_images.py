import requests
import argparse
import re
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,  # or DEBUG, WARNING, ERROR
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Docker Hub API
BASE_URL = "https://hub.docker.com/v2"
# https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
SEMVER = re.compile("^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$")

def get_token(username, password):
    response = requests.post(f"{BASE_URL}/users/login/", json={
        "username": username,
        "password": password
    })
    response.raise_for_status()
    return response.json()["token"]

def get_tags(token, repository):
    headers = {"Authorization": f"JWT {token}"}
    url = f"{BASE_URL}/repositories/{repository}/tags?page_size=100"
    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        url = data.get("next")
        for t in data["results"]:
            yield t

def delete_tag(token, repository, tag):
    headers = {"Authorization": f"JWT {token}"}
    url = f"{BASE_URL}/repositories/{repository}/tags/{tag}/"
    response = requests.delete(url, headers=headers)
    if response.status_code == 204:
        logging.info(f"Deleted tag: {tag}")
    else:
        logging.warning(f"Failed to delete tag: {tag} - {response.status_code} {response.text}")

def match_release(tag):
    match = SEMVER.match(tag)
    return match is not None and (match.group("prerelease") is None or match.group("prerelease").startswith("SNAPSHOT")) and match.group("buildmetadata") is None

def main():
    parser = argparse.ArgumentParser(description="Remove prerelease tags from the given images", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("org", help="Docker hub organization/user that holds the image", type=str)
    parser.add_argument("image", help="Image from which to remove tags", type=str)
    parser.add_argument("user", help="Dockerhub user for login", type=str)
    parser.add_argument("token", help="Dockerhub authorization token", type=str)
    parser.add_argument("--months", dest="months", help="Number of months for which the prerelease images are kept", type=int, default=2)
    args = parser.parse_args()

    now = datetime.today() - timedelta(days= args.months * 30)

    token = get_token(args.user, args.token)
    tags = get_tags(token, f"{args.org}/{args.image}")

    for tag_info in tags:
        tag_name = tag_info["name"]
        tag_date = datetime.strptime(tag_info["last_updated"], "%Y-%m-%dT%H:%M:%S.%fZ")
        if tag_date < now and not match_release(tag_name):
            logging.info(f"Deleting tag: {tag_name} (last updated: {tag_date})")
            delete_tag(token, f"{args.org}/{args.image}", tag_name)

if __name__ == "__main__":
    main()
