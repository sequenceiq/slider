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
  
# Hoya Release 0.8.0

December 2013

This release is built against the Apache Hadoop 2.2.0 and hbase 0.96.0-hadoop2
artifacts. 


## Key changes


### Command line parsing reworked

How the command line is parsed has changed, with some implications

1. In previous versions, parameters that were not used by a specific action would
be ignored. Now an unknown parameter (such as `--wait` in the `list` action) results
in an error.

2. The action which you wish to perform MUST be the first item on the command line.
This is similar to how `git` works: the action defines the set of options that
the command line supports. 

  BAD:  `hoya --filesystem hdfs://rhel:9090 thaw cluster1`
  GOOD: `hoya thaw cluster1 --filesystem hdfs://rhel:9090`
        
3. You no longer have to list the cluster you wish to work with immediately
after the action.

  GOOD: `hoya thaw cluster1 --filesystem hdfs://rhel:9090`
  GOOD: `hoya thaw --filesystem hdfs://rhel:9090 cluster1`

These changes have resulted in significant changes inside the Hoya code itself
-hopefully this will ease maintenance in future, as it will now enable us
to add new commands more easily. It will also immediately highlight problems
in the command lines passed to Hoya

### More and stricter exit codes.

The `EXIT_CLUSTER_IN_USE` (74) is now raised whenever the named cluster
is in use when an action is requested which requires a cluster to not be running.
Examples : thaw, destroy, create

### `hoya status` command supports a `--out` argument

You can direct the JSON output of the `hoya status` command into a file -as
this excludes all the other logged information, this is the way to retrieve
a JSON specification that can be parsed.

Example

    hoya status cluster1 --out cluster1.json
    
### Cleanup of cached lib files used in thawing/creating clusters

Hoya copies its JAR files into `/${user.home}/.hoya/tmp/${clustername}/${app-id}`
in order to launch the Hoya Application Master -until this release they were
not cleaned up.

Now, whenever an cluster is thawed, all existing temporary files saved on
previous cluster launches are deleted.

### A new `version` action

The command `hoya version` will print out information about the compiled
Hoya application, the version of Hadoop against which it was built -and
the version of Hadoop that is currently on its classpath.