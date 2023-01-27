#!/bin/sh

if test -e patch.diff ; then
  rm -f patch.diff
  echo patch.diff was removed
fi

BRANCH=$(git rev-parse --abbrev-ref HEAD)
RUNID=$(gh run list -b "$BRANCH" -w "Build and Test" --json databaseId -q .[0].databaseId)

if gh run download -n patch $RUNID ; then
  git apply patch.diff
fi
