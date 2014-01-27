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

### The `killcontainer` command will kill a container in a cluster

    killcontainer cl1 container_1390413725233_0001_01_000004

This does not update the desired state of the cluster, and so will trigger
the Hoya AM to request a replacement container.

This command exists for failure injection in functional tests. 

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