#!/bin/sh

if test -e patch-csharp.diff ; then
  rm -f patch-csharp.diff
  echo patch-csharp.diff was removed
fi

BRANCH=$(git rev-parse --abbrev-ref HEAD)
RUNID=$(gh run list -b "$BRANCH" -w "Code Formatting" --json databaseId -q .[0].databaseId)

if gh run download -n patch-csharp $RUNID ; then
  git apply patch-csharp.diff
fi
