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

TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
DAYS="${DAYS:-7}"

ensure_local_python

args=(--days "$DAYS" --db "$ROOT_DIR/var/ghactions.db")
if [[ -n "$TOKEN" ]]; then
  args+=(--token "$TOKEN")
fi

exec "$PYTHON_BIN" "$TOOLS_DIR/backfill.py" "${args[@]}" "$@"
