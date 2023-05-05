#!/usr/bin/env bash


REPO=$(gh repo view --json url -q '.url')

while IFS= read RUN_ID ; do
  gh run rerun --failed $RUN_ID
  echo $REPO/actions/runs/$RUN_ID
done < <(gh run list -b main -w "Build and Test" --json conclusion,databaseId -q 'map(select(.conclusion=="failure")) | .[].databaseId')

while IFS= read BRANCH ; do
  echo
  echo $BRANCH
  echo
  while IFS= read RUN_ID ; do
    if [[ "$RUN_ID" != "" ]] ; then
      gh run rerun --failed $RUN_ID
      echo $REPO/actions/runs/$RUN_ID
    fi
  done < <(gh run list -b $BRANCH -w "Build and Test" --json conclusion,databaseId -q '.[0] | if .conclusion=="failure" then .databaseId else "" end')
done < <(gh pr list -A @me --json headRefName --jq .[].headRefName)

