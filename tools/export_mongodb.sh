#!/bin/bash

while IFS= read COLLECTION ; do
  echo $COLLECTION
  docker exec database mongoexport --quiet -d database -c $COLLECTION > $COLLECTION.json
done < <(docker exec database mongosh database --eval 'db.getCollectionNames()' --quiet --json | grep '"' | tr -d '", \r')
