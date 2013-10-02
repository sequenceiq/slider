#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# this is the shell script to start Hoya deploying an application
# Usage: hoya [--config confdir] <action> <commands>

function usage
{
  echo "Usage: hoya [--config confdir]  <action> <arguments>"
  echo ""
  echo "Most commands print help when invoked w/o parameters."
}

# Hoya works out its own location 
this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$bin/$script"

# lib directory is one up; it is expected to contain 
# hoya.jar and any other dependencies that are not in the
# standard Hadoop classpath

libdir="${bin}/"

# plan
# if: --confdir is set, set env dir HADOOP_CONF_DIR

# HADOOP_PREFIX is then basedir 

export HADOOP_CLASSPATH="${libdir}:\*.jar"
launcher=org.apache.hadoop.yarn.service.launcher.ServiceLauncher

hadoop ${launcher} org.apache.hadoop.hoya.Hoya
