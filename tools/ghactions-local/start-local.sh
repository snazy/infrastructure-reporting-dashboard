#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$TOOLS_DIR/env.sh"

CONFIG_FILE="$ROOT_DIR/reporting-dashboard.yaml"
TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"

ensure_local_python

if [[ ! -f "$CONFIG_FILE" ]]; then
  cat > "$CONFIG_FILE" <<EOF
server:
  bind: 127.0.0.1
  port: 8080
  error_reporting: json
  max_form_size: 1048576
  rate_limit_per_ip: 0
reporting:
  userid:
    valid_userid_syntax: '^[a-zA-Z0-9_.-]+$'
  uptime:
    series: {}
  mailstats:
    hosts: []
github:
  read_token: "$TOKEN"
  datadir: "$VAR_DIR"
EOF
  echo "Created $CONFIG_FILE"
fi

if [[ ! -f "$VAR_DIR/ghactions.db" ]]; then
  echo "No $VAR_DIR/ghactions.db found. Run tools/ghactions-local/run-backfill.sh first for local GHA data."
fi

exec "$PYTHON_BIN" "$TOOLS_DIR/server_local.py"
