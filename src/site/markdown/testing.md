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

     The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL
      NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and
      "OPTIONAL" in this document are to be interpreted as described in
      RFC 2119.

## Standalone Tests

Hoya core contains a suite of tests that are designed to run on the local machine,
using Hadoop's `MiniDFSCluster` and `MiniYARNCluster` classes to create small,
one-node test clusters. All the YARN/HDFS code runs in the JUnit process; the
AM and spawned HBase and Accumulo processes run independently.

## Functional Tests

The functional test suite is designed to run the executables against
a live cluster

## Configuration of functional tests

Maven needs to be given 
1. A path to the expanded test archive
1. A path to a hoya configuration directory for the cluster

The path for the expanded test is automatically calculated as being the directory under
`..\hoya-assembly\target` where an untarred hoya distribution can be found.
If it is not present, the tests will fail

The path to the configuration directory must be supplied in the property
`hoya.conf.dir` which can be set on the command line

    mvn test -Dhoya.conf.dir=src/test/configs/sandbox/hoya

It can also be set in the (optional) file `hoya-funtest/build.properties`:

    hoya.conf.dir=src/test/configs/sandbox/hoya

This file is loaded whenever a hoya build or test run takes place

## Configuration of `hoya-client.xml`

The `hoya-client.xml` must have extra configuration options for both the HBase and
Accumulo tests, as well as a common set fr

### Non-mandatory options

The following test options may be added to `hoya-client.xml` if the defaults
need to be changed
                   
    <property>
      <name>hoya.test.zkhosts</name>
      <description>comma separated list of ZK hosts</description>
      <value>localhost</value>
    </property>
       
    <property>
      <name>hoya.test.thaw.wait.seconds</name>
      <description>Time to wait in seconds for a thaw to result in a running AM</description>
      <value>60000</value>
    </property>
    
    <property>
      <name>hoya.test.freeze.wait.seconds</name>
      <description>Time to wait in seconds for a freeze to halt the cluster</description>
      <value>60000</value>
    </property>
            
     <property>
      <name>hoya.test.timeout.seconds</name>
      <description>Time to in seconds before a test is considered to have failed.
      There are some maven properties which also define limits and may need adjusting</description>
      <value>180000</value>
    </property>
    
    
    
Note that while the same properties need to be set in
`hoya-core/src/test/resources/hoya-client.xml`, those tests take a file in the local
filesystem -here a URI to a path visible across all nodes in the cluster are required
the tests do not copy the .tar/.tar.gz files over. The application configuration
directories may be local or remote -they are copied into the `.hoya` directory
during cluster creation.

### HBase Parameters

The HBase tests can be enabled or disabled
    
    <property>
      <name>hoya.test.hbase.enabled</name>
      <description>Flag to enable/disable HBase tests</description>
      <value>true</value>
    </property>
        
Mandatory test parameters must be added to `hoya-client.xml`

  
    <property>
      <name>hoya.test.hbase.tar</name>
      <description>Path to the HBase Tar file in HDFS</description>
      <value>hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz</value>
    </property>
    
    <property>
      <name>hoya.test.hbase.appconf</name>
      <description>Path to the directory containing the HBase application config</description>
      <value>file://${user.dir}/src/test/configs/sandbox/hbase</value>
    </property>
    
Optional parameters:  
  
     <property>
      <name>hoya.test.hbase.launch.wait.seconds</name>
      <description>Time to wait in seconds for HBase to start</description>
      <value>180000</value>
    </property>  


#### Accumulo configuration options

Enable/disable the tests

     <property>
      <name>hoya.test.accumulo.enabled</name>
      <description>Flag to enable/disable Accumulo tests</description>
      <value>true</value>
     </property>
         
         
Optional parameters
         
     <property>
      <name>hoya.test.accumulo.launch.wait.seconds</name>
      <description>Time to wait in seconds for Accumulo to start</description>
      <value>180000</value>
     </property>



### Testing against a secure cluster

To test against a secure cluster

1. `hoya-client.xml` must be configured as per [Security](security.html).
1. the client must have the kerberos tokens issued so that the user running
the tests has access to HDFS and YARN.

If there are problems authenticating (including the cluster being offline)
the tests appear to hang

### Validating the configuration

    mvn test  -Dtest=TestBuildSetup


## Parallel execution

Attempts to run test cases in parallel failed -even with a configuration
to run methods in a class sequentially, but separate classes independently.

Even after identifying and eliminating some unintended sharing of static
mutable variables, trying to run test cases in parallel seemed to hang
tests and produce timeouts.

For this reason parallel tests have been disabled. To accelerate test runs
through parallelization, run different tests on different hosts instead.

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

## Running and debugging the functional tests.

The functional tests all 

1. In the root `hoya` directory, build a complete Hoya release

        mvn install -DskipTests
1. Start the YARN cluster/set up proxies to connect to it, etc.

1. In the `hoya-funtest` dir, run the test

        mvn test -Dtest=TestHBaseCreateCluster
        
A common mistake during development is to rebuild the `hoya-core` JARs
then the `hoya-funtest` tests without rebuilding the `hoya-assembly`.
In this situation, the tests are in sync with the latest build of the code
-including any bug fixes- but the scripts executed by those tests are
of a previous build of `hoya-core.jar`. As a result, the fixes are not picked
up.

#### To propagate changes in hoya-core through to the funtest classes for
testing, you must build/install the hoya packages from the root assembly.

## Limitations of hoya-funtest

1. All tests run from a single client -workload can't scale
1. Output from failed AM and containers aren't collected