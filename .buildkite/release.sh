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

#npm config delete _auth;
#npm config set _auth $TOKEN

export MAVEN_OPTS="-Xmx8G -Xss128M -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:+CMSClassUnloadingEnabled"
release_opts=
if [ -n "$RELEASE_VERSION" ]; then
release_opts="$release_opts -DreleaseVersion=$RELEASE_VERSION"
fi
if [ -n "$NEXT_VERSION" ]; then
release_opts="$release_opts -DdevelopmentVersion=$NEXT_VERSION"
fi

# This step also push the merged change to the Uber pinot
# Common arguments for skipping tests, GPG, etc.
# NOTE: Do NOT add -Dmaven.test.skip=true here. That flag skips test compilation
# AND prevents maven-jar-plugin:test-jar from producing test-jars. Several modules
# (e.g. pinot-input-format) depend on pinot-spi's test-jar, so it must be built.
# -DskipTests=true alone is sufficient to skip test execution while still compiling
# test sources and packaging test-jars.
# -T1C massively speeds up builds by parallelizing 1 thread per CPU core but messes up output logs.
# Remove it if you want to see logs in order.
SKIP_ARGS=(
  -Dgpg.skip=true
  -Drat.skip=true
  -Dlicense.skip=true
  -DskipTests=true
  -Dsurefire.skip=true
  -Dfailsafe.skip=true
  -Dmaven.javadoc.skip=true
  -Denforcer.skip=true
  -T1C
  -Dmaven.artifact.threads=16
)

# Build the arguments string that will be passed to the FORKED Maven process.
# The parent POM (apache-37) hardcodes <arguments> in the release plugin config,
# which causes -Darguments=... on the CLI to be ignored. We override <arguments>
# in pinot's pom.xml to use ${release.arguments}, so we pass the value via that
# property instead.
RELEASE_ARGUMENTS="$(printf '%s ' "${SKIP_ARGS[@]}") -Daether.connector.basic.parallelPut=false -P build-shaded-jar"

# Skip GPG signing - run prepare only
$MVN_CMD -e -B release:clean release:prepare \
-Dgpg.skip=true \
-Denforcer.skip=true \
-DsignTag=false \
-DsignCommit=false \
-DpreparationGoals="clean validate" \
-Drelease.arguments="$RELEASE_ARGUMENTS" \
$release_opts


# Perform: checkout the tag and run deploy
# Use useReleaseProfile=false to avoid apache-release profile GPG signing
# -Drelease.arguments passes skip flags to the FORKED Maven that release:perform spawns.
# -Dgoals must only contain goal names (not -D properties).
# Top-level SKIP_ARGS affect only the outer Maven running release:perform itself.
$MVN_CMD -e -B release:perform \
-DuseReleaseProfile=false \
-Drelease.arguments="$RELEASE_ARGUMENTS" \
-Dgoals=deploy

