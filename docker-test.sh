#!/bin/bash

set -euo pipefail
set -x

docker build -t ras-stac .
docker run --rm -it -w /plugins ras-stac pytest
