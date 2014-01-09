<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
  
# Testing

# Standalone Tests

Hoya core contains a suite of tests that are designed to run on the local machine,
using Hadoop's `MiniDFSCluster` and `MiniYARNCluster` classes to create small,
one-node test clusters. All the YARN/HDFS code runs in the JUnit process; the
AM and spawned HBase and Accumulo processes run independently.

# Functional Tests

The functional test suite is designed to run the executables against
a live cluster

## Configuration of functional tests

Maven needs to be given 
1. A path to the expanded test archive
1. A path to a hoya configuration directory for the cluster


    hoya-funtest $ mvn test -Dhoya.conf.dir=src/test/configs/sandbox/hoya

## Parallel execution

see [Maven docs](http://maven.apache.org/surefire/maven-surefire-plugin/examples/fork-options-and-parallel-execution.html)
1. Maven is set up to run test case classes in parallel, *but not the individual tests*.
1. Java7+ does not place any guarantees on the ordering of test methods within 
a test case.

