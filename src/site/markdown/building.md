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


*Important*: As of October 6, 2013, Maven 3.1 is not supported due to
[version issues](https://cwiki.apache.org/confluence/display/MAVEN/AetherClassNotFound).

## Building a compatible Hadoop version

During development, Hoya is built against a local version of Hadoop branch 2.1.x,
so that we can find and fix bugs in Hadoop as well in Hoya.

It will fall back to downloading the `-SNAPSHOT` artifacts from the ASF snapshot
repository

To build and install locally, check out apache svn/github, branch `2.1-beta` 


export HADOOP_VERSION=2.2.0-rc0

Build and install it locally, skipping the tests:

    mvn install -DskipTests

You have to do this every morning to avoid the ASF nightly artifacts being picked up/

To make a tarball for use in test runs:

    #On  osx
    mvn clean package -Pdist -Dtar -DskipTests -Dmaven.javadoc.skip=true 
    
    # on linux
    mvn package -Pdist -Pnative -Dtar -DskipTests -Dmaven.javadoc.skip=true 

Then expand this

    pushd hadoop-dist/target/
    gunzip hadoop-$HADOOP_VERSION.tar.gz 
    tar -xvf hadoop-$HADOOP_VERSION.tar 
    popd

This creates an expanded version of Hadoop. You can now actually run Hadoop
from this directory

## building a compatible HBase version

Checkout the HBase `trunk` branch from apache svn/github.  

    git clone git://git.apache.org/hbase.git
    git remote rename origin apache
    
The maven command for building hbase artifacts against this hadoop version is 

    mvn clean install assembly:single -DskipTests -Dmaven.javadoc.skip=true -Dhadoop.profile=2.0 -Dhadoop-two.version=$HADOOP_VERSION


    
This will create `hbase-0.97.0-SNAPSHOT.tar.gz` in the directory `hbase-assembly/target/` in
the hbase source tree. 

    export HBASE_VERSION=0.97.0-SNAPSHOT
    export HBASE_VERSION=0.96.0
    
    pushd hbase-assembly/target
    gunzip hbase-$HBASE_VERSION-bin.tar.gz 
    tar -xvf hbase-$HBASE_VERSION-bin.tar
    gzip hbase-$HBASE_VERSION-bin.tar
    popd

This will create an untarred directory containing
hbase. Both the `.tar.gz` and untarred file are needed for testing. Most
tests just work directly with the untarred file as it saves time uploading
and downloading then expanding the file.

(and if you set `HBASE_VERSION` to something else, you can pick up that version
-making sure that hoya is in sync)

    


For more information (including recommended Maven memory configuration options),
see [HBase building](http://hbase.apache.org/book/build.html)

For building just the JAR files:

    mvn clean install -DskipTests -Dhadoop.profile=2.0 -Dhadoop-two.version=$HADOOP_VERSION

*Tip:* you can force set a version in Maven by having it update all the POMs:

    mvn versions:set -DnewVersion=0.97.1-SNAPSHOT

## Building Accumulo

Clone accumulo from apache; check out trunk

If needed, patch the POM file to depend on the version of Hadoop you are building
locally, by changing the `hadoop.version` property



In the accumulo project directory:

    mvn clean package -Passemble -DskipTests -Dmaven.javadoc.skip=true \
     -Dhadoop.profile=2.0  -Dhadoop.version=HADOOP_VERSION

This creates an accumulo tar.gz file in `assemble/target/`. Unzip then untar
this, to create a .tar file and an expanded directory

    accumulo/assemble/target/accumulo-1.6.0-SNAPSHOT-bin.tar
    
 This can be done with the command sequence
    
    pushd assemble/target/
    gunzip -f accumulo-1.6.0-SNAPSHOT-bin.tar.gz 
    tar -xvf accumulo-1.6.0-SNAPSHOT-bin.tar 
    popd
    
Note that the final location of the accumulo files is needed for the configuration,
it may be directly under target/ or it may be in a subdirectory, with 
a patch such as `target/accumulo-1.6.0-SNAPSHOT-dev/accumulo-1.6.0-SNAPSHOT/`


## Testing

### Configuring Hoya to locate the relevant artifacts

You must have the file `src/test/resources/hoya-test.xml` (this
is ignored by git), declaring where HBase, accumulo, Hadoop and zookeeper are:

    <configuration>
    
      <property>
        <name>hoya.test.hbase.home</name>
        <value>/Users/hoya/hbase/hbase-assembly/target/hbase-0.97.0-SNAPSHOT</value>
        <description>HBASE Home</description>
      </property>
    
      <property>
        <name>hoya.test.hbase.tar</name>
        <value>/Users/hoya/hbase/hbase-assembly/target/hbase-0.97.0-SNAPSHOT-bin.tar.gz</value>
        <description>HBASE archive URI</description>
      </property> 
         
      <property>
        <name>hoya.test.accumulo_home</name>
        <value>/Users/hoya/accumulo/assemble/target/accumulo-1.6.0-SNAPSHOT-dev/accumulo-1.6.0-SNAPSHOT/</value>
        <description>Accumulo Home</description>
      </property>
    
      <property>
        <name>hoya.test.accumulo_tar</name>
        <value>/Users/hoya/accumulo/assemble/target/accumulo-1.6.0-SNAPSHOT-bin.tar.gz</value>
        <description>Accumulo archive URI</description>
      </property>
      
      <property>
        <name>zk.home</name>
        <value>
          /Users/hoya/Apps/zookeeper</value>
        <description>Zookeeper home dir on target systems</description>
      </property>
    
      <property>
        <name>hadoop.home</name>
        <value>
          /Users/hoya/hadoop-trunk/hadoop-dist/target/hadoop-2.1.2-SNAPSHOT</value>
        <description>Hadoop home dir on target systems</description>
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



## Releasing


Most of the release process is automated, though some manual steps are needed
at the end to get the binaries up and any announcements out


### Before you begin

Check out the code, run the tests. This should be done on a checked out
version of the code that is not the one you are developing on
(ideally, a clean VM), to ensure that you aren't releasing a slightly
modified version of your own, and that you haven't accidentally
included passwords or other test run details into the build resource
tree.

If the tests fail, don't do a release.


### Set the release version

After selecting a version (such as 0.5.1), set that across the project




verify that this version has been picked up with a

    mvn clean
  
The version  you have just set must be listed at the start of the build.
  
### Building the actual Release


In the top level hoya directory

    mvn clean site:site site:stage package -DskipTests
  
This does a clean build of the application and its release
artifacts. It does not run the tests: we do not want the
tests to be included in the redistributable packages.


### Afterwards
  

Move up to a new build version -make it a -SNAPSHOT/development release

    mvn --batch-mode release:update-versions -DdevelopmentVersion=0.5.2
    mvn clean
  

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

## Groovy 

Hoya uses Groovy 2.x as its language for writing tests -for better assertions
and easier handling of lists and closures. Although the first prototype
used Groovy on the production source, this was dropped in favor of
a Java-only production codebase.

## Maven utils


Here are some handy aliases to make maven easier 

    alias mci='mvn clean install -DskipTests'
    alias mi='mvn install -DskipTests'
    alias mvct='mvn clean test'
    alias mvnsite='mvn site:site -Dmaven.javadoc.skip=true'
    alias mvt='mvn test'


### dumping the dependencies

    mvn dependency:tree 
    
Have a look at this after adding an JAR to the classpath to look out
for spurious dependency pickup. There's a lot of inconsistencies
between Hadoop, HBase and Accumulo to watch out for
