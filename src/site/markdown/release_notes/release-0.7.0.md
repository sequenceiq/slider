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
  
# Hoya Release 0.6.6

November 2013

This release is built against the Apache Hadoop 2.2.0 and hbase 0.96.0-hadoop2
artifacts. 


Key changes

## Automatic archive path fixup: --version command dropped

In previous versions of hoya, unless the name of the directory 
in the `hbase.tar` or `accumulo.tar` file matched that of the
version Hoya was set to expect at compile time,
the path to the expanded subdirectory had to be set when creating a cluster,
using the `--version` keyword.

This is no longer the case and the argument has been dropped. Instead the
application master will look inside the expanded archive and determine for itself
what the path is -and fail if it cannot locate `bin/hbase` or `bin/accumulo` under
the single directory permitted in the archive.

This also means that when an HBase or accumulo archive is updated to a later version,
that new version will be picked up automatically.

### Monitor command

The `monitor \<clustername>` command monitors a cluster to verify that

1. it is running at startup
2. it stays running

The monitor operations are a basic "is the YARN application running"
alongside any service specific ones, such as HTTP and RPC port scanning,
other liveness operations. These are the same operations that will be used
in the AM itself to monitor cluster health.

Consult the man page document for usage instructions

### 'max' option for container RAM or cores.

As well as being able to specify the numeric values of memory and cores
in role, via the `--roleopt` argument, you can now ask for the maximum
allowed value by using the parameter `max`

Here are an example options to set the maximum cores and RAM for a master
node

    --roleopt master yarn.vcores max
    --roleopt master yarn.memory max

This option can not be used for the `hoya` role, as the container limits
are not known until the AM itself starts up and can retrieve them.

The limits are, however, not provided in the `info` section of a cluster
status file returned by querying the HoyaAM for is current status -which
can be done via the `hoya status \<clustername>` command.

Here is an example of an `info` section from a test cluster:

    "info" : {
       "live.time" : "Thu Nov 21 16:15:25 GMT 2013",
       "live.time.millis" : "1385050525110",
       "create.time" : "1385050525110",
       "status.time" : "Thu Nov 21 16:15:28 GMT 2013",
       "status.time.millis" : "1385050525127",
       "master.address" : "rack4node5:4096",
       "yarn.vcores" : "16",
       "yarn.memory" : "4096"
     }

These values can be used to set the limits on future AM requirements.


### Failure limits on container restarts

There is an inital implementation of failure tracking, in which the Hoya cluster
terminates itself if it concludes that the cluster cannot start or work reliably.

1. The cluster option `"hoya.container.failure.threshold` takes an integer
value declaring the maximum number of failures tolerated for any specific role.

1. When the number of failed roles exceeds this threshold *for any reason*.
the cluster terminates itself, with error code "73" appearing in the
diagnostics message.

This is just the initial failure resilience code -expect more.