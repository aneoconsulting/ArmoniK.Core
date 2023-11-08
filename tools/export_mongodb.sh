#!/bin/bash

while IFS= read COLLECTION ; do
  echo $COLLECTION
  docker exec -t database mongoexport -d database -c $COLLECTION -o - > $COLLECTION.json
done < <(docker exec -it database mongosh database --eval 'db.getCollectionNames()' --quiet --json | grep '"' | tr -d '", \r')