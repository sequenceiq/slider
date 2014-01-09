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
1. Test suites must be designed to work even when executed in parallel with
other test suites.
1. Tests within a test suite *do not need to be designed to work in parallel
with other tests in the same suite*

This leads to a design where every test suite/test class must be designed to
work with its own Hoya cluster instance within a single, shared YARN cluster,
in parallel with other Hoya clusters -but the tests within the suite
can expect to be run one at a time, albeit in an unknown order.

## Other constraints

* Port assignments SHOULD NOT be fixed, as this will cause clusters to fail if
there are too many instances of a role on a same host, or if other tests are
using the same port.
* If a test does need to fix a port, it MUST be for a single instance of a role,
and it must be different from all others. The assignment should be set in 
`org.apache.hoya.funtest.itest.PortAssignments` so as to ensure uniqueness
over time. Otherwise: use the value of `0` to allow the OS to assign free ports
on demand.

## Test Requirements


1. Test cases should be written so that each class works with exactly one
Hoya-deployed cluster
1. Every test MUST have its own cluster name -preferably derived from the
classname.
1. This cluster should be deployed in an `@BeforeClass` method.
1. The `@AfterClass` method MUST tear this cluster down.
1. Tests within the suite (i.e. class) must be designed to be independent
-to work irrespectively of the ordering of other tests.

