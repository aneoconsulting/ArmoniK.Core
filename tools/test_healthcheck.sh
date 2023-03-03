#!/bin/bash
set -e

if [ $# -ne 1 ]; then
  echo This script expects only one parameter : the service to restart
  exit 1
fi

SERVICE=$1

just stop $SERVICE
just healthChecks
just restoreDeployment $SERVICE
sleep 10
just healthChecks
