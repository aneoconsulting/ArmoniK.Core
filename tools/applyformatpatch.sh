#!/bin/sh

rm -f patch.diff

BRANCH=$(git rev-parse --abbrev-ref HEAD)
RUNID=$(gh run list -b "$BRANCH" -w "Build and Test" --json databaseId -q .[0].databaseId)

if gh run download -n patch $RUNID ; then
  git apply patch.diff
fi
