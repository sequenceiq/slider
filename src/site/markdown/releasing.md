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


# Releasing Hoya

Here is our release process.

### Before you begin

Check out the code, run the tests. This should be done on a checked out
version of the code that is not the one you are developing on
(ideally, a clean VM), to ensure that you aren't releasing a slightly
modified version of your own, and that you haven't accidentally
included passwords or other test run details into the build resource
tree.


**Step #1:** Create a JIRA for the release, estimate 3h
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

As the test run takes 30-60+ minutes, now is a good time to consider
creating the release notes of step 9


**Step #8:** Look in `hoya-assembly/target` to find the `.tar.gz` file, and the
expanded version of it. Inspect that expanded version to make sure that
everything looks good -and that the versions of all the dependent artifacts
look good too: there must be no `-SNAPSHOT` dependencies.


**Step #9: deploy Hoya against a live cluster

1. Expand the .tar.gz file
1. Use it to create, flex, freeze then thaw a Hoya application on a remote YARN cluster
1. Do this for both HBase and Accumulo

**Step #10:** Create a a one-line plain text release note for commits and tags
And a multi-line markdown release note, which will be used for artifacts.


Release of Hoya against hadoop 2.2.0 and hbase 0.96.0-hadoop2

This release of Hoya:

* Is built against the (ASF staged) hadoop 2.2.0 and hbase 0.96.0-hadoop2 artifacts. 
* Supports Apache HBase cluster creation, flexing, freezing and thawing.
* Contains the initial support of Apache Accumulo: all accumulo roles
can be created, though its testing is currently very minimal.
* Has moved `log4j.properties` out of the JAR file and into the directory
`conf/`, where it will be picked up both client-side and server-side.
Enjoy!


**Step #11:** Finish the git flow release, either in the SourceTree GUI or
the command line:

    
    git flow release finish hoya-$HOYA_RELEASE
    

On the command line you have to enter the one-line release description
prepared earlier.

You will now be back on the `develop` branch.

**Step #12:** Switch back to `develop` and update its version number past
the release number


    export HOYA_RELEASE=0.6.0-SNAPSHOT
    mvn versions:set -DnewVersion=$HOYA_RELEASE
    git commit -a -m "BUG-XYZ updating development POMs to $HOYA_RELEASE"

**Step #13:** Push the release and develop branches to github 
(We recommend naming the hortonworks github repository 'hortonworks' to avoid
 confusion with apache, personal and others):

    git push hortonworks master develop 

(if you are planning on any release work of more than a single test run,
 consider having your local release branch track the master)

The `git-flow` program automatically pushes up the `release/hoya-X.Y` branch,
before deleting it locally.


**Step #14:** ### For releasing small artifacts

(This only works for files under 5GB)
Browse to https://github.com/hortonworks/hoya/releases/new

Create a new release on the site by following the instructions


**Step #15:**  For releasing via an external CDN (e.g. Rackspace Cloud)

Using the web GUI for your particular distribution network, upload the
`.tar.gz` artifact


**Step #16:** Announce the release 

**Step #17:** Get back to developing!

Check out the develop branch and purge all release artifacts

    git checkout develop
    git pull hortonworks
    mvn clean
    
