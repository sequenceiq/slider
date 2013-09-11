<!---
~~ Licensed under the Apache License, Version 2.0 (the "License");
~~ you may not use this file except in compliance with the License.
~~ You may obtain a copy of the License at
~~
~~   http://www.apache.org/licenses/LICENSE-2.0
~~
~~ Unless required by applicable law or agreed to in writing, software
~~ distributed under the License is distributed on an "AS IS" BASIS,
~~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
~~ See the License for the specific language governing permissions and
~~ limitations under the License. See accompanying LICENSE file.
-->

# Building Hoya

Hoya is currently built with some unreleased Apache Artifacts, because it
uses Hadoop 2.1 and needs a version of HBase built against that.

Here's how to set this up.

## Before you begin

You will need a version of Maven 3.0, set up with enough memory

    MAVEN_OPTS=-Xms256m -Xmx512m -Djava.awt.headless=true


*Important*: As of September 10, 2013, Maven 3.1 is not supported due to
[https://cwiki.apache.org/confluence/display/MAVEN/AetherClassNotFound](version issues).

## Building a compatible Hadoop version

During development, Hoya is built against a local version of Hadoop branch 2.1.x,
so that we can find and fix bugs in Hadoop as well in Hoya.

It will fall back to downloading the `-SNAPSHOT` artifacts from the ASF snapshot
repository

To build and install locally, check out apache svn/github, branch `2.1-beta` 

Build and install it locally, skipping the tests:

    mvn install -DskipTests

You have to do this every morning to avoid the ASF nightly artifacts being picked up/

To make a tarball for use in test runs:

    #On  osx
    mvn package -Pdist -Dtar -DskipTests -Dmaven.javadoc.skip=true 
    
    # on linux
    mvn package -Pdist -Pnative -Dtar -DskipTests -Dmaven.javadoc.skip=true 

This takes time so try to avoid doing this.

## building a compatible HBase version

Checkout the HBase `hbase-0.95` branch from apache svn/github.  

The maven command for building hbase artifacts with 2.1.0-beta is 

    mvn clean install assembly:single -DskipTests -Dmaven.javadoc.skip=true -Dhadoop.profile=2.0 -Dhadoop-two.version=2.1.0-beta 

For building just the JAR files:

    mvn clean install -DskipTests -Dhadoop.profile=2.0 -Dhadoop-two.version=2.1.0-beta
    
This will create `hbase-0.95.3-SNAPSHOT.tar.gz` in the directory `hbase-assembly/target/` in
the hbase source tree. 

In that directory `hbase-assembly/target/`

    gunzip hbase-0.95.3-SNAPSHOT-bin.tar.gz 
    tar -xvf hbase-0.95.3-SNAPSHOT-bin.tar 

This will create an untarred directory `hbase-0.95.3-SNAPSHOT-bin` containing
hbase. Both the `.tar.gz` and untarred file are needed for testing. Most
tests just work directly with the untarred file as it saves time uploading
and downloading then expanding the file.

For more information (including recommended Maven memory configuration options),
see [http://hbase.apache.org/book/build.html](HBase building)

*Tip:* you can force set a version in Maven by having it update all the POMs:

    mvn versions:set -DnewVersion=0.95.4-SNAPSHOT

## Building Accumulo

    mvn clean package -Passemble -DskipTests -Dhadoop.profile=2.0

This creates an accumulo tar.gz file in `assemble/target/`. Unzip then untar
this, to create a .tar file and an expanded directory

    accumulo/assemble/target/accumulo-1.6.0-SNAPSHOT-bin.tar.gz

## Testing

The hbase tarball needs to be unzipped somewhere and defined for Hoya to pick up

You must have the file `src/test/resources/hoya-test.xml` (this
is ignored by git), declaring where HBase is:

    <configuration>
    
      <property>
        <name>hoya.test.hbase.home</name>
        <value>/Users/hoya/hbase/hbase-assembly/target/hbase-0.95.3-SNAPSHOT</value>
        <description>HBASE Home</description>
      </property>
    
      <property>
        <name>hoya.test.hbase.tar</name>
        <value>/Users/hoya/hbase/hbase-assembly/target/hbase-0.95.3-SNAPSHOT-bin.tar.gz</value>
        <description>HBASE archive URI</description>
      </property> 
         
      <property>
        <name>hoya.test.accumulo_home</name>
        <value>/Users/hoya/hbase/hbase-assembly/target/accumulo-1.6.0-SNAPSHOT-bin.tar</value>
        <description>HBASE Home</description>
      </property>
    
      <property>
        <name>hoya.test.accumulo_tar</name>
        <value>/Users/hoya/accumulo/assemble/target/accumulo-1.6.0-SNAPSHOT-bin.tar.gz</value>
        <description>HBASE archive URI</description>
      </property>
    
    </configuration>
    

## Debugging a failing test

1. Locate the directory `target/$TESTNAME` where TESTNAME is the name of the 
test case and or test method. This directory contains the Mini YARN Cluster
logs. For example, `TestLiveRegionService` stores its data under 
`target/TestLiveRegionService`

1. Look under that directory for `-logdir` directories, then an application
and container containing logs. There may be more than node being simulated;
every node manager creates its own logdir.

1. Look for the `out.txt` and `err.txt` files for stdout and stderr log output.

1. Hoya uses SLF4J to log to `out.txt`; remotely executed processes may use
either stream for logging

Example:

    target/TestLiveRegionService/TestLiveRegionService-logDir-nm-1_0/application_1376095770244_0001/container_1376095770244_0001_01_000001/out.txt

1. The actual test log from JUnit itself goes to the console and into 
`target/surefire/`; this shows the events happening in the YARN services as well
 as (if configured) HDFS and Zookeeper. It is noisy -everything after the *teardown*
 message happens during cluster teardown, after the test itself has been completed.
 Exceptions and messages here can generally be ignored.
 
This is all a bit complicated -debugging is simpler if a single test is run at a
time, which is straightforward

    mvn clean test -Dtest=TestLiveRegionService


### Building the JAR file

You can create the JAR file and set up its directories with

     mvn package -DskipTests

This copies all the dependencies to `target/lib`. The JAR has a manifest set
to pull this in, so you can just go `java -jar target/hoya-0.3-SNAPSHOT.jar org.apache.hadoop.hoya.Hoya`

## Attn OS/X developers

YARN on OS/X doesn't terminate subprocesses the way it does on Linux, so
HBase Region Servers created by the hbase shell script remain running
even after the tests terminate.

This causes some tests -especially those related to flexing down- to fail, 
and test reruns may be very confused. If ever a test fails because there
are too many region servers running, this is the likely cause

After every test run: do a `jps -v` to look for any leftover HBase services
-and kill them.

Here is a handy bash command to do this

    jps -l | grep HRegion | awk '{print $1}' | xargs kill -9

# Notes

Hoya uses Groovy 2.x as its language for writing tests -for better assertions
and easier handling of lists and closures. Although the first prototype
used Groovy on the production source, this has been dropped in favor of
a Java-only codebase. We do still push up `groovyall.jar` to the classpath
of the HoyaAM.