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
  
# Hoya Release 0.11.1

January 2014

This release is built against the Apache Hadoop 2.2.0 and hbase 0.96.1.1-hadoop2
artifacts. 


## Key changes


### Client user name used for HDFS operations in insecure clusters

In a non-Kerberos cluster, the client's username is now propagated
to the Application Master and all containers -and is used to 
identify the cluster for HDFS access. 

As a result, the created databases will correctly belong to the owner
of the cluster, not the identity of the account which yarn applications
run under. 

We've reverted the default cluster security permissions on created directories
to 0750 as appropriate. 

### Cluster status request lists container IDs

The container IDs of running containers are now listed in the output of
the `status` operation:

      "instances" : {
        "worker" : [ "container_1390413725233_0001_01_000003", "container_1390413725233_0001_01_000002" ],
        "hoya" : [ "container_1390413725233_0001_01_000001" ],
        "master" : [ "container_1390413725233_0001_01_000004" ]
      },
      
These can be used in debugging cluster behavior.

### Hoya builds against HBase-0.98.0

Hoya has switched to HBase version 0.98.0, as we required some minor
changes in the HBase load test framework in order to use it against
Hoya-managed HBase clusters.

If/when the final HBase 0.98.0 release is made, hoya will build directly
against the public maven artifacts. Until then, HBase 0.98.0 must
be built locally, as described in the `building Hoya` document.

*Important* this does not imply that Hoya can no longer deploy HBase 0.96
--only that Hoya needs it to build, and it includes the hbase-0.98.0
JAR files in its `lib/` directory.

### New client configuration option, `hoya.yarn.restart.limit`

This places a limit on how many times the Hoya Application Master can be
started before the application is considered to have failed.

On long-lived applications, a high value ensures that the application master will
be robust against failures of the underlying servers.


### The `killcontainer` command will kill a container in a cluster

    killcontainer cl1 container_1390413725233_0001_01_000004

This does not update the desired state of the cluster, and so will trigger
the Hoya AM to request a replacement container.

This command exists for failure injection in functional tests. 

### the `am-suicide` command will cause the Hoya Application master to exit

This command instructs the Hoya Application Master to exit, and so trigger
YARN's response to such a failure.

This command exists for failure injection in functional tests. 

### Hoya has (disabled at compile time) support for Hadoop 2.4 AM restarts

Hadoop 2.4 addss a feature [YARN-1489](https://issues.apache.org/jira/browse/YARN-1489),
in which an Application Master ("AM") can request that YARN preserve all running
containers in the event of an AM failure -and supply this list to the replacement
instance of the AM for it to manage.

Hoya uses this feature to allow the HBase or Accumulo cluster to keep running
*even when the Application Master has failed*. As such, it removes the
AM failure as a source of a (transient) outage of the running application.

This feature is disabled, as enabling it prevents Hoya from running on a
Hadoop 2.2 cluster. There is a branch in the source repository 
[which enables this feature](https://github.com/hortonworks/hoya/tree/feature/BUG-12943-hadoop-2.4-support). Anyone wishing to explore the feature must
check out this branch, and build and test it against locally built
versions of Hadoop 2.4.