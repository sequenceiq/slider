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
  
# Hoya Model

This is the model of Hoya and YARN for the rest of the specification.

### File System

A File System `HDFS` represents a Hadoop FileSystem -either HDFS or another File
System which spans the cluster. There are also other filesystems that
can act as sources of data that is then copied into HDFS. These will be marked
as `FS` or with the generic `FileSystem` type.


There's ongoing work in [HADOOP-9361][https://issues.apache.org/jira/browse/HADOOP-9361]
to define the Hadoop Filesytem APIs using the same notation as here,
the latest version being available on [github][https://github.com/steveloughran/hadoop-trunk/tree/stevel/HADOOP-9361-filesystem-contract/hadoop-common-project/hadoop-common/src/site/markdown/filesystem]

Two key references are

 1. [The notation reused in the Hoya specifications][https://github.com/steveloughran/hadoop-trunk/blob/stevel/HADOOP-9361-filesystem-contract/hadoop-common-project/hadoop-common/src/site/markdown/filesystem/notation.md]
 1. [The model of the filesystem][https://github.com/steveloughran/hadoop-trunk/blob/stevel/HADOOP-9361-filesystem-contract/hadoop-common-project/hadoop-common/src/site/markdown/filesystem/model.md]
 
 The model and its predicates and invariants will be used in these specifications.
 
### YARN

The YARN runtime is a state, `YARN`, comprised of (Apps, Queues:, Nodes)

    Apps:    Map[AppId, Apps]
        App: (Name, Type, YarnApplicationState, AmContainer, AmRPC, AmWebURI)
        YarnApplicationState : {NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED }
    Nodes:  Map[nodeID,(name, containers:List[ContainerID])] 
    Queues: List[Queue]
        Queue:  List[Requests]
        Request = {
          launch(app-name, app-type, requirements, localisedresources, command)
          add-container(appId, priority, requirements)
        }
    
      