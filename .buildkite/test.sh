#!/bin/bash
set -ex
#
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
#

# Source common Java and Maven setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/init.sh"

TOKEN=$(echo -n "ignored:$UNPM_TOKEN" | base64 -w 0)
echo -e "\n" >> ~/.npmrc
echo "//unpm.uberinternal.com/:_auth=$TOKEN" >> ~/.npmrc

mvn install -DskipTests -Dlicense.skip=true -Drat.ignoreErrors=true -Pbin-dist -T 1C
mvn test -pl 'pinot-spi,pinot-clients,pinot-plugins,pinot-connectors,pinot-server,pinot-minion,pinot-broker,pinot-distribution,pinot-segment-spi,pinot-tools,pinot-perf,pinot-integration-test-base,pinot-compatibility-verifier,pinot-query-planner,pinot-query-runtime' -Drat.ignoreErrors=true -T 1C
