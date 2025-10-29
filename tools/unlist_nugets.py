import requests
import argparse
import re
import dateutil.parser
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta, timezone

# https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
SEMVER = re.compile("^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$")

# NuGet API endpoints
CATALOG_INDEX_URL = "https://api.nuget.org/v3/catalog0/index.json"
DELETE_URL_TEMPLATE = "https://www.nuget.org/api/v2/package/{package_id}/{version}"
REGISTRATION_URL_TEMPLATE = "https://api.nuget.org/v3/registration5-gz-semver2/{package_id}/index.json"

def match_release(tag):
    return SEMVER.match(tag) is not None and (SEMVER.match(tag).group("prerelease") is None or SEMVER.match(tag).group("prerelease").startswith("SNAPSHOT")) and SEMVER.match(tag).group("buildmetadata") is None

def process_catalog_page(page_url, package_id):
    response = requests.get(page_url)
    response.raise_for_status()
    page = response.json()

    for item in page["items"]:
        if item.get("@type", "") == "Package":
            catalog_entry = item.get("catalogEntry", {})
            version = catalog_entry.get("version")
            published = catalog_entry.get("published")
            nuget_id = catalog_entry.get("id")
            listed = catalog_entry.get("listed")
            if listed and version and published and nuget_id.lower() == package_id.lower():
                yield {
                    "version": version,
                    "published": published
                }
        elif item.get("@type", "") == "catalog:CatalogPage":
            yield from process_catalog_page(item["@id"], package_id)


def get_versions_with_dates(package_id):
    yield from process_catalog_page(REGISTRATION_URL_TEMPLATE.format(package_id=package_id.lower()), package_id)


def unlist_version(package_id, version, api_key):
    url = DELETE_URL_TEMPLATE.format(package_id=package_id, version=version)
    headers = {"X-NuGet-ApiKey": api_key}
    response = requests.delete(url, headers=headers)
    if response.status_code == 200:
        print(f"Unlisted {package_id} {version}")
    else:
        print(f"Failed to unlist {package_id} {version}: {response.status_code} {response.text}")

def main():
    parser = argparse.ArgumentParser(description="Unlist prerelease tags for the given nuget package", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("nuget", help="NuGet package ID", type=str)
    parser.add_argument("token", help="Nuget API token that allows unlisting packages", type=str)
    parser.add_argument("--months", dest="months", help="Number of months for which the prerelease nugets are kept", type=int, default=3)
    args = parser.parse_args()

    cutoff_date = datetime.now(timezone.utc) - timedelta(days= args.months * 30)
    items = get_versions_with_dates(args.nuget)

    for item in items:
        version = item["version"]
        published = datetime.fromisoformat(item["published"])
        if not match_release(version) and published < cutoff_date:
            print(f"Deleting version: {version} (published: {published})")
            unlist_version(args.nuget, version, args.token)

if __name__ == "__main__":
    main()
