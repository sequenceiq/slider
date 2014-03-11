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
  
# Hoya Release 0.13.1

March 2014

This release is built against the Apache Hadoop 2.3.0, HBase-0.98.0RC1
and Accumulo 1.5.0 artifacts. 

Download: [hoya-0.13.1-all.tar.gz](http://dffeaef8882d088c28ff-185c1feb8a981dddd593a05bb55b67aa.r18.cf1.rackcdn.com/hoya-0.13.1-all.tar.gz)


## Key changes

### Built against Hadoop 2.3.0

This release has been built using the Hadoop 2.3.0 libraries.

*This should not affect the ability to deploy and run on Hadoop 2.2.0 clusters*

As part of our release process we run functional tests of Hoya on a HDP-2.0
cluster running Hadoop 2.2.0, as well as a secure Hadoop 2.3.0 cluster.

### Cluster flex operation *always* persists the changes.

This is needed to ensure that when an AM is restarted by YARN, it picks
up the latest cluster specification, not the original one used when
the cluster was thawed. 

It also addresses a minor issue wherea second flex operation forgets
the values from a preceeding flex.

The `--persist` parameter is still supported -it is just ignored.


### Improved Hoya AM web UI

The Web UI has more features, including the JSON status of
a cluster.

## Status includes more detail on running instances
 
The `/status/live` section in status reports lists the running cluster instances by role,
including the command used to start the instance, as well as the
environment variables. 

As an example, here is the status on a running worker node:

    "status": {
      "live": {
        "worker": {
          "container_1394032374441_0001_01_000003": {
            "name": "container_1394032374441_0001_01_000003",
            "role": "worker",
            "roleId": 1,
            "createTime": 1394032384451,
            "startTime": 1394032384503,
            "released": false,
            "host": "192.168.1.88",
            "state": 3,
            "exitCode": 0,
            "command": "hbase-0.98.0/bin/hbase --config $PROPAGATED_CONFDIR regionserver start 1><LOG_DIR>/region-server.txt 2>&1 ; ",
            "diagnostics": "",
            "environment": [
              "HADOOP_USER_NAME=\"hoya\"",
              "HBASE_LOG_DIR=\"/tmp/hoya-hoya\"",
              "HBASE_HEAPSIZE=\"256\"",
              "MALLOC_ARENA_MAX=\"4\"",
              "PROPAGATED_CONFDIR=\"$PWD/propagatedconf\""
            ]
          }
        }
        failed : {}
      }

# Warning: Changes to persistent cluster specification likely in future

We've been redesiging how we're going to persist the cluster specification.

These changes will *not* be backwards compatible -but are intended to be
stable after this. We will put the plans up online for review shortly

