#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../flink-job"
mvn -DskipTests package
