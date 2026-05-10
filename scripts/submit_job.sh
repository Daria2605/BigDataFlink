#!/usr/bin/env bash
set -euo pipefail
docker compose exec jobmanager flink run -d /opt/flink/usrlib/bigdata-flink-star-job-1.0.0.jar
