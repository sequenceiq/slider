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


Hoya is built against Hadoop release 2.2.0; 

During development, its convenient (but not mandatory)
to have a local version of Hadoop -so that we can find and fix bugs/add features in
Hadoop as well in Hoya.


To build and install locally, check out apache svn/github, branch `release-2.2.0`
 
This is a tag; git will encourage you to create a branch from the tagged
commit, which is recommended.


For the scripts below, set the `HADOOP_VERSION` variable to the version

    export HADOOP_VERSION=2.2.0

Build and install it locally, skipping the tests:

    mvn install -DskipTests

You have to do this every morning to avoid the ASF nightly artifacts being picked up/

To make a tarball for use in test runs:

    #On  osx
    mvn clean install package -Pdist -Dtar -DskipTests -Dmaven.javadoc.skip=true 
    
    # on linux
    mvn package -Pdist -Pnative -Dtar -DskipTests -Dmaven.javadoc.skip=true 

Then expand this

    pushd hadoop-dist/target/
    gunzip hadoop-$HADOOP_VERSION.tar.gz 
    tar -xvf hadoop-$HADOOP_VERSION.tar 
    popd

This creates an expanded version of Hadoop. You can now actually run Hadoop
from this directory. Do note that unless you have the native code built for
your target platform, Hadoop will be slower. For installations, use
the Apache Hadoop 2.2 package or Hortownworks HDP-2.0.

## building a compatible HBase version

Checkout the HBase `trunk` branch from apache svn/github.  

    git clone git://git.apache.org/hbase.git
    git remote rename origin apache
    git fetch --tags apache
    git checkout hbase-0.96.0 --
    git checkout -b hbase-0.96.0
    
    
The maven command for building hbase artifacts against this hadoop version is 

    mvn clean install assembly:single -DskipTests -Dmaven.javadoc.skip=true -Dhadoop.profile=2.0 -Dhadoop-two.version=$HADOOP_VERSION


    
This will create `hbase-0.96.0.tar.gz` in the directory `hbase-assembly/target/` in
the hbase source tree. 

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

    mvn clean install -Passemble -DskipTests -Dmaven.javadoc.skip=true \
     -Dhadoop.profile=2.0  -Dhadoop.version=$HADOOP_VERSION

This creates an accumulo tar.gz file in `assemble/target/`. Unzip then untar
this, to create a .tar file and an expanded directory

    accumulo/assemble/target/accumulo-1.6.0-SNAPSHOT-bin.tar
    
 This can be done with the command sequence
    
    export ACCUMULO_VERSION=1.6.0-SNAPSHOT
    
    pushd assemble/target/
    gunzip -f accumulo-$ACCUMULO_VERSION-bin.tar.gz 
    tar -xvf accumulo-$ACCUMULO_VERSION-bin.tar 
    popd
    
Note that the final location of the accumulo files is needed for the configuration,
it may be directly under target/ or it may be in a subdirectory, with 
a patch such as `target/accumulo-ACCUMULO_VERSION-dev/accumulo-ACCUMULO_VERSION/`


## Testing

### Configuring Hoya to locate the relevant artifacts

You must have the file `src/test/resources/hoya-test.xml` (this
is ignored by git), declaring where HBase, accumulo, Hadoop and zookeeper are:

    <configuration>
    
      <property>
        <name>hoya.test.hbase.home</name>
        <value>/Users/hoya/hbase/hbase-assembly/target/hbase-0.96.0</value>
        <description>HBASE Home</description>
      </property>
    
      <property>
        <name>hoya.test.hbase.tar</name>
        <value>/Users/hoya/hbase/hbase-assembly/target/hbase-0.96.0-bin.tar.gz</value>
        <description>HBASE archive URI</description>
      </property> 
         
      <property>
        <name>hoya.test.accumulo_home</name>
        <value>/Users/hoya/accumulo/assemble/target/accumulo-1.6.0-SNAPSHOT/</value>
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
          /Users/hoya/hadoop-trunk/hadoop-dist/target/hadoop-2.2.0</value>
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

We do not use maven release plug in. Why not?
Use the release plugin and then come back and ask that question.

Here then is our release process.

### Before you begin

Check out the code, run the tests. This should be done on a checked out
version of the code that is not the one you are developing on
(ideally, a clean VM), to ensure that you aren't releasing a slightly
modified version of your own, and that you haven't accidentally
included passwords or other test run details into the build resource
tree.


**Step #1:** Create a JIRA for the release, estimate  2h
(so you don't try to skip the tests)

**Step #2:** Check everything in. Git flow won't let you progress without this.

**Step #3:** Git flow: create a release branch

    export HOYA_RELEASE=0.5.2
    
    git flow release start hoya-$HOYA_RELEASE

**Step #4:** in the new branch, increment those version numbers using (the maven
versions plugin)[http://mojo.codehaus.org/versions-maven-plugin/]

    mvn versions:set -DnewVersion=$HOYA_RELEASE


**Step #5:** commit the changed POM files
  
        git add <changed files>
        git commit -m "BUG-XYZ updating release POMs for $HOYA_RELEASE"


  
**Step #6:** Do a final test run to make sure nothing is broken
**Step #7:** Build the release package

These two stages can be merged into one, which will result in the 
test results being included as a project report for each module.
    
    mvn clean site:site site:stage package 

As the test run takes 30+ minutes, now is a good time to consider
creating the release notes of step 9


**Step #8:** Look in `hoya-assembly/target` to find the `.tar.gz` file, and the
expanded version of it. Inspect that expanded version to make sure that
everything looks good -and that the versions of all the dependent artifacts
look good too: there must be no `-SNAPSHOT` dependencies.


**Step #9:** Create a a one-line plain text release note for commits and tags
And a multi-line markdown release note, which will be used for artifacts.


    Release of Hoya against hadoop 2.2.0 and hbase 0.96.0-hadoop2

    This release of Hoya:
    
    1. Is built against the (ASF staged) hadoop 2.2.0 and hbase 0.96.0-hadoop2 artifacts. 
    1. Supports Apache HBase cluster creation, flexing, freezing and thawing.
    1. Contains the initial support of Apache Accumulo: all accumulo roles
    can be created, though its testing is currently very minimal.
    1. Has moved `log4j.properties` out of the JAR file and into the directory
    `conf/`, where it will be picked up both client-side and server-side.
    Enjoy!


**Step #10:** Finish the git flow release, either in the SourceTree GUI or
the command line:

    
    git flow release finish hoya-$HOYA_RELEASE
    

On the command line you have to enter the one-line release description
prepared earlier.

You will now be back on the `develop` branch.

**Step #11:** Switch back to develop and update its version number past
the release number


    export HOYA_RELEASE=0.6.0-SNAPSHOT
    mvn versions:set -DnewVersion=HOYA_RELEASE
    git commit -a -m "BUG-XYZ updating development POMs to HOYA_RELEASE"

**Step #12:** Push the release and develop branches to github 
(We recommend naming the hortonworks github repository 'hortonworks' to avoid
 confusion with apache, personal and others):

    git push hortonworks master develop 

(if you are planning on any release work of more than a single test run,
 consider having your local release branch track the master)

The `git-flow` program automatically pushes up the `release/hoya-X.Y` branch,
before deleting it locally.


**Step #13:** ### For releasing small artifacts

(This only works for files under 5GB)
Browse to https://github.com/hortonworks/hoya/releases/new

Create a new release on the site by following the instructions


**Step #14:**  For releasing via an external CDN (e.g. Rackspace Cloud)

Using the web GUI for your particular distribution network, upload the
`.tar.gz` artifact


**Step #15:** Announce the release 

**Step #16:** Get back to developing!

Check out the develop branch and purge all release artifacts

    git checkout develop
    git pull hortonworks
    mvn clean
    


# Development Notes


## Git branch model

The git branch model uses is
(Git Flow)[http://nvie.com/posts/a-successful-git-branching-model/].

This is a common workflow model for Git, and built in to
(Atlassian Source Tree)[http://sourcetreeapp.com/].
 
The command line `git-flow` tool is easy to install 
 
    brew install git-flow
 
or

    apt-get install git-flow
 
You should then work on all significant features in their own branch and
merge them back in when they are ready.

 
    # until we get a public JIRA we're just using an in-house one. sorry
    git flow feature start BUG-8192
    
    # finishes merges back in to develop/
    git flow feature finish BUG-8192
    
    # release branch
    git flow release start 0.4.0
    
    git flow release finish 0.4.0
    
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

## Debugging

You can hook an IDE up to Hoya from the `hoya-core` package

* target to run first `mvn package -DskipTests` in `hoya-core`
* Main Class `org.apache.hadoop.hoya.Hoya`
* Recommended JVM Args `-Xmx256m -Dlog4j.configuration=org/apache/hadoop/hoya/log4j.properties`

