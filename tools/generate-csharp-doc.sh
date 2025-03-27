#!/bin/sh

set -e

dotnet tool install -g docfx
dotnet build
docfx docfx.json
sed -E -i 's/([#]+) <a id="([^"]+)"><\/a> (.+)/<a id="\2"><\/a>\n\1 \3/g' .docs/content/csharp/*.md
