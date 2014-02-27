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
  
# Cluster Specification

### Notation: 

In this document, a full path to a value is represented as a path 
`options/zookeeper.port`  ; an assigment as  `options/zookeeper.port=2181`.

A wildcard indicates all entries matching a path: `options/zookeeper.*`


## History

The Hoya cluster specification was implicitly defined in the file
`org.apache.hoya.api.ClusterDescription`. It had a number of roles

1. Persistent representaton of cluster state
1. Internal model of desired cluster state within the Application Master.
1. Dynamic representation of current cluster state when the AM
was queried, marshalled over the network as JSON.
1. Description of updated state when reconfiguring a running cluster.

Initially the dynamic status included a complete history of all containers
-this soon highlit some restrictions on the maximum size of a JSON-formatted
string in Hadoop's "classic" RPC: 32K, after which the string was silently
truncated. Accordingly, this history was dropped.

Having moved to Protocol Buffers as the IPC wire format, with a web view
alongside, this history could be reconsidered.

The initial design place most values into the root entry, and relied
on Jaxon introspection to set and retrieve the values -it was a
Java-first specification, with no external specificatin or regression tests.

As the number of entries in the root increased, the design switched to storing
more attributes into specific sections *under* the root path:

* `info`: read-only information about the cluster.
* `statistics`: Numeric statistics about the cluster

# Sections

## `info {}`

Read-only list of information about the application. Generally this is
intended to be used for debugging and testing.

### Persisted: static information about the file history
 
  "info" : {
    "create.hadoop.deployed.info" : "(detached from release-2.3.0) @dfe46336fbc6a044bc124392ec06b85",
    "create.application.build.info" : "Hoya Core-0.13.0-SNAPSHOT Built against commit# 1a94ee4aa1 on Java 1.7.0_45 by stevel",
    "create.hadoop.build.info" : "2.3.0",
    "create.time.millis" : "1393512091276",
  },
 
 
### Dynamic: 
 
 
 whether the AM supports service restart without killing all the containers hosting
 the role instances:
    "hoya.am.restart.supported" : "false",
    
    
 timestamps of the cluster going live, and when the status query was made   
    "live.time" : "27 Feb 2014 14:41:56 GMT",
    "live.time.millis" : "1393512116881",
    "status.time" : "27 Feb 2014 14:42:08 GMT",
    "status.time.millis" : "1393512128726",
    
  yarn data provided to the AM  
    "yarn.vcores" : "32",
    "yarn.memory" : "2048",
  
  information about the application and hadoop versions in use. Here
  the application was built using Hadoop 2.3.0, but is running against the version
  of Hadoop built for HDP-2.
  
    "status.application.build.info" : "Hoya Core-0.13.0-SNAPSHOT Built against commit# 1a94ee4aa1 on Java 1.7.0_45 by stevel",
    "status.hadoop.build.info" : "2.3.0",
    "status.hadoop.deployed.info" : "bigwheel-m16-2.2.0 @704f1e463ebc4fb89353011407e965"
 
 ## `statistics`
 
 Statistics on each role. 
 
 They can be divided into counters that only increase

    "containers.start.completed": 0,
    "containers.start.failed": 0,
    "containers.failed": 0,
    "containers.completed": 0,
    "containers.requested": 0

and those that vary depending upon the current state

    "containers.live": 0,
    "containers.active.requests": 0,
    "containers.desired": 0,


* Propose: move these values out of statistics into some other section, as they
are state, not statistics*
 
     "statistics": {
       "worker": {
         "containers.start.completed": 0,
         "containers.live": 0,
         "containers.start.failed": 0,
         "containers.active.requests": 0,
         "containers.failed": 0,
         "containers.completed": 0,
         "containers.desired": 0,
         "containers.requested": 0
       },
       "hoya": {
         "containers.unknown.completed": 0,
         "containers.start.completed": 0,
         "containers.live": 1,
         "containers.start.failed": 0,
         "containers.failed": 0,
         "containers.completed": 0,
         "containers.surplus": 0
       },
       "master": {
         "containers.start.completed": 0,
         "containers.live": 0,
         "containers.start.failed": 0,
         "containers.active.requests": 0,
         "containers.failed": 0,
         "containers.completed": 0,
         "containers.desired": 0,
         "containers.requested": 0
       }
     },
    
