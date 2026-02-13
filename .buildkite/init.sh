#!/bin/bash
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

# Setup the JAVA path
setup_env () {
    export JAVA_HOME=$JAVA_HOME_PATH
    export PATH=$JAVA_HOME/bin:$PATH
    echo java --version
}

# Download the JAVA 11
setup_java () {
	rm -rf $JAVA_HOME_PATH || true
    if [[ ! -d $JAVA_HOME_PATH ]]; then
        mkdir -p $JAVA_HOME_PATH
    fi

    BINARY_ARCHIVE_NAME="$(echo $JAVA_JDK_NUMBER | sed 's/-//g').tar.gz"
    ARCHIVE_INSTALL_PATH="$JAVA_HOME_PATH/$BINARY_ARCHIVE_NAME"
    DOWNLOAD_SUCCEEDED=false

    wget "http://artifactory.uber.internal:4587/artifactory/libs-release-local/com/uber/devxp/jdk-linux/${JAVA_JDK_NUMBER}/jdk-linux-${JAVA_JDK_NUMBER}.tar.gz" -P ${ARCHIVE_INSTALL_PATH}
    if [[ "$?" -eq 0 ]]; then
        DOWNLOAD_SUCCEEDED=true
    fi

    if [[ $DOWNLOAD_SUCCEEDED == "false" ]]; then
        echo "Unable to download JDK $JAVA_JDK_NUMBER"
        return
    else
        echo "Download JDK $JAVA_JDK_NUMBER succeeded"
    fi
    tar -xvzf $ARCHIVE_INSTALL_PATH/*.tar.gz -C $JAVA_HOME_PATH --strip 1 1>/dev/null 2>&1
}

JAVA_JDK_NUMBER="11.0.11_9"
JAVA_HOME_PATH="$HOME/java_home/$JAVA_JDK_NUMBER"

if [[ -f "$JAVA_HOME_PATH/bin/java" ]]; then
    echo "JDK 11 was already downloaded"
else
    echo "JDK 11 will be downloaded"
    setup_java
fi
setup_env

# Download and setup Maven 3.9.12
MAVEN_VERSION="3.9.12"
MAVEN_HOME="$HOME/maven/$MAVEN_VERSION"
setup_maven () {
    rm -rf $MAVEN_HOME || true
    mkdir -p $MAVEN_HOME
    wget "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" -O /tmp/maven.tar.gz
    tar -xzf /tmp/maven.tar.gz -C $MAVEN_HOME --strip-components=1
    rm /tmp/maven.tar.gz
}

if [[ -f "$MAVEN_HOME/bin/mvn" ]]; then
    echo "Maven $MAVEN_VERSION already installed"
else
    echo "Installing Maven $MAVEN_VERSION"
    setup_maven
fi
export PATH=$MAVEN_HOME/bin:$PATH
export MVN_CMD="$MAVEN_HOME/bin/mvn"
