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

## Building a compatible Hadoop version

During development, Hoya is built against a local version of Hadoop branch 2.1.x,
so that we can find and fix bugs in Hadoop as well in Hoya.

It will fall back to downloading the `-SNAPSHOT` artifacts from the ASF snapshot
repository

To build and install locally, check out apache svn/github, branch `2.1-beta` 

Build and install it locally, skipping the tests:

    mvn install -DskipTests

You have to do this every morning to avoid the ASF nightly artifacts being picked up/

## building a compatible HBase version

Checkout the HBase `hbase-0.94` branch from apache svn/github.  

The maven command for building hbase artifacts with hadoop-2.1 is 

    mvn clean -Dhadoop.profile=2.0 -Dhadoop.version=2.1.0-SNAPSHOT -DskipTests package

This will create hbase-0.94.9-SNAPSHOT.tar.gz in the directory target within
the hbase-0.94 source. That's your hbase with hadoop-2.1


# Testing


You must have the file `src/test/resources/hoya-test.xml` (this
is ignored by git), declaring where HBase is:

    <property>
      <name>hoya.test.hbase_home</name>
      <value>/Users/stevel/Java/Apps/hbase</value>
      <description>HBASE Home</description>
    </property>
    
## Attn OS/X developers

YARN on OS/X doesn't terminate subprocesses the way it does on Linux, so
HBase Region Servers created by the hbase shell script remain running
even after the tests terminate.

This causes some tests -especially those related to flexing down- to fail, 
and test reruns may be very confused. If ever a test fails because there
are too many region servers running, this is the likely cause

After every test run: do a `jps -v` to look for any leftover HBase services
-and kill them.

# Notes

Hoya uses Groovy 2.x as its language for writing tests -for better assertions
and easier handling of lists and closures. Although the first prototype
used Groovy on the production source, this has been dropped in favor of
a Java-only codebase. We do still push up `groovyall.jar` to the classpath
of the HoyaAM.