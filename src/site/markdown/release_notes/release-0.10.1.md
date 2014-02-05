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
  
# Hoya Release 0.10.1

January 2014

This release is built against the Apache Hadoop 2.2.0 and hbase 0.96.1.1-hadoop2
artifacts. 


## Key changes

### YARN queue for the application can be specified

The configuration parameter `hoya.yarn.queue` can now be set
in the `hoya-client.xml` file to name the queue; `hoya.yarn.queue.priority`
is an integer defining priority within the queue (0 is the highest priority)

    <property>
      <name>hoya.yarn.queue</name>
      <value>background</value>
    </property>

    <property>
      <name>hoya.yarn.queue</name>
      <value>10</value>
    </property>

or on the command line via the `-D` option:

    bin/hoya thaw cl1  -D hoya.yarn.queue=interactive -D hoya.yarn.queue.priority=5 
  
  
### the `freeze` command now supports a `--message` parameter


    bin/hoya freeze cl1  --message "shutdown for maintenance"
  
### the `freeze` command now supports a `--force` parameter

A forced freeze bypasses the Hoya Application Master for a forced shutdown,
irrespective of the state of the application. The client will communicate
directly with the YARN infrastructure to kill the application.

    bin/hoya freeze cl1  --force 

As Hoya is designed for clusters to be killed without warning (as are HBase
and Accumulo), a forced freeze should not be significantly different from
a normal freeze -an operation which issues an RPC request to the Hoya Application
Master to terminate all allocated containers.

### `exists` command behavior changed

1. `exists` succeeds if the cluster is defined in the filesystem
1. Clusters that are not in the filesystem result in a return code of `70`:
   unknown cluster
1. if the `--live` argument is supplied, the cluster must be running.
1. The return code if `--live` was specified but the cluster is frozen is`-1`.
That includes the case where the cluster has failed.

These changes allow scripts to look for a cluster being defined

    hoya exists cluster4 && echo "cluster 4 is defined"
 
And to declare that a cluster must be running

    hoya exists cluster4 --live || exit

### Package `org.apache.hadoop.hoya` renamed `org.apache.hoya`

This is in preparation for incubating the project in the Apache Incubator.

1. logging configuration in `log4j.properties` may need to be updated.
1. It may break debugger configurations and other entry points into the code.
1. It will stop existing role history files being read in. This should
be ignored by Hoya; these files are a hint to the location in
a YARN cluster of the data used previously. If there is any problem, 
delete the directory `~/.hoya/cluster/${cluster}/history/`


### Configuration options for cluster directory permissions now set in `hoya-client.xml`

As part of (ongoing) work to ensure data access in non-Kerberized clusters,
directory permissions for a hoya cluster are set when the cluster is
created, using permissions set in `hoya.cluster.directory.permissions`
and `hoya.data.directory.permissions`

Expect more improvements in operations in "insecure" YARN clusters in future
releases.

### Methods in `HoyaClient` class made visible and more directly programmable

To aid in using the `HoyaClient` class as a library for working with Hoya clusters,
the `actionBuild` and `actionCreate` operations are now public. All `action` methods
now take their parsed command line arguments as parameters along with -where appropriate-
the cluster name.

Note that this is is not a stable API -having a public library with clean separation
of concerns is preferable. The changes in this release are an interim step.

### JVM heap size is passed down to HBase and Accumulo.

The role option `jvm.heapsize` is now passed down to HBase and Accumulo

    --roleopt worker jvm.heapsize 1G
    --roleopt master jvm.heapsize 256M
    
