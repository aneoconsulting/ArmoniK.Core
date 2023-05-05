from re import compile
import subprocess
import argparse

parser = argparse.ArgumentParser(
    description="Set the version of the ArmoniK.Api* packages to the new version",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("new_version", help="New version", type=str)
args = parser.parse_args()

# git command to list all the csproj files from the project.
cmd = subprocess.Popen('git ls-tree -r HEAD --name-only --full-tree | grep "csproj$"', shell=True, stdout=subprocess.PIPE, text=True)
cmd.wait()
files = cmd.communicate()[0].rstrip("\n").split("\n")

# regex for finding the packages to update
# This regex allows to separate the version from the line with groups.
regex = compile(r'(<\s*PackageReference\s+Include\s*=\s*"ArmoniK\.Api\.[a-zA-Z]+"\s+Version\s*=\s*").*("\s*\/>)')

for file in files:
    print(file)

    with open(file, "r+") as f:
        # We replace the versions in the matching line.
        # We use groups to create a new line from the existing content and we replace the old version with the new one.
        lines = [regex.sub(fr'\g<1>{args.new_version}\g<2>', line) for line in f.readlines()]
        f.seek(0)
        f.truncate(0)
        f.writelines(lines)
