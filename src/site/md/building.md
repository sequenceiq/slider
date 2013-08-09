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

  ---
  Hoya ${project.version} -
  ---
  ---
  ${maven.build.timestamp}

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