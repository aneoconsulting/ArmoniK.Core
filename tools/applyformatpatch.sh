#!/bin/sh

BRANCH=$(git rev-parse --abbrev-ref HEAD)
RUNID=$(gh run list -b "$BRANCH" -w "Code Formatting" --json databaseId -q .[0].databaseId)

for lang in csharp rust ; do
  if test -e patch-$lang.diff ; then
    rm -f patch-$lang.diff
    echo patch-$lang.diff was removed
  fi

  if gh run download -n patch-$lang $RUNID ; then
    git apply patch-$lang.diff
  fi
done
