#!/bin/bash

set -euo pipefail

if [ "$#" -ne 5 ]; then
  echo "usage: $0 <classify_result> <images_required> <manager_result> <coordinator_result> <coordinator_go_result>" >&2
  exit 2
fi

classify_result="$1"
images_required="$2"
manager_result="$3"
coordinator_result="$4"
coordinator_go_result="$5"

if [ "$classify_result" != "success" ]; then
  echo "images: fail - classification did not succeed (classify=${classify_result})"
  exit 1
fi

if [ "$images_required" = "true" ]; then
  if [ "$manager_result" = "success" ] && [ "$coordinator_result" = "success" ] && [ "$coordinator_go_result" = "success" ]; then
    echo "images: pass - all image jobs succeeded"
    exit 0
  fi
  echo "images: fail - image jobs required but not all succeeded (manager=${manager_result} coordinator=${coordinator_result} coordinator-go=${coordinator_go_result})"
  exit 1
fi

if [ "$images_required" = "false" ]; then
  if [ "$manager_result" = "skipped" ] && [ "$coordinator_result" = "skipped" ] && [ "$coordinator_go_result" = "skipped" ]; then
    echo "images: pass - image jobs intentionally skipped for docs-only change"
    exit 0
  fi
  echo "images: fail - image jobs should have been skipped (manager=${manager_result} coordinator=${coordinator_result} coordinator-go=${coordinator_go_result})"
  exit 1
fi

echo "images: fail - unknown images_required value (${images_required})"
exit 1
