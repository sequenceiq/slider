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

## File System

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
 
## YARN

From the perspective of YARN application, The YARN runtime is a state, `YARN`, 
comprised of (Apps, Queues, Nodes)

    Apps:    Map[AppId, ApplicationReport]
        App: (Name, report: ApplicationReport, Requests:List[AmRequest])
          ApplicationReport: AppId, Type, User, YarnApplicationState, AmContainer, AmRPC, AmWebURI,
          YarnApplicationState : {NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED }
          AmRequest = { add-container(priority, requirements), release(containerId)}
    Nodes:  Map[nodeID,(name, containers:List[ContainerID])] 
    Queues: List[Queue]
        Queue:  List[Requests]
        Request = {
          launch(app-name, app-type, requirements, context)
        }
        Context: (localisedresources, command)

This is doesn't completely model the cluster from the AM perspective -there's no
notion of node operations (launching code in a container) or events coming from YARN.


### Operations & predicates used the specifications


    def applications(YARN, type) = 
        [ app.report for app in YARN.Apps.values where app.report.Type == type]
    
    def user-applications(YARN, type, user)
        [a in applications(YARN, type) where: a.User == user]

## UserGroupInformation

Applications are launched and executed on hosts computers: either client machines
or nodes in the cluster, these have their own state which may need modeling


    HostState: Map[String, String]

A key part of the host state is actually the identity of the current user,
which is used to define the location of the persistent state of the cluster -including
its data, and the identity under which a deployed container executes.

In a secure cluster, this identity is accompanied by kerberos tokens that grant the caller
access to the filesystem and to parts of YARN itself.

This specification does not currently explicitly model the username and credentials.
If it did they would be used throughout the specification to bind to a YARN or HDFS instance.

### UserGroupInformation.getCurrentUser(): UserGroupInformation

Returns the current user information. This information is immutable and fixed for the duration of the process.



## Hoya

### Cluster name

A valid cluster name is a name of length > 1 which follows the internet hostname scheme of letter followed by letter or digit

    def valid-cluster-name(c) =
        len(c)> 0
        and c[0] in ['a'..'z']
        and c[1] in (['a'..'z'] + ['-'] + ['0..9']) 

### Persistent Cluster State

A Hoya cluster's persistent state is stored in a path

    def cluster-path(FS, clustername) = user-home(FS) + ["clusters", clustername]
    def cluster-json-path(FS, clustername) = cluster-path(FS, clustername) + ["cluster.json"]
    def original-conf-path(FS, clustername) = cluster-path(FS, clustername) + ["original"] 
    def generated-conf-path(FS, clustername) = cluster-path(FS, clustername) + ["generated"]
    def data-path(FS, clustername) = cluster-path(FS, clustername) + ["data"]

When a cluster is built/created the specified original configuration directory
is copied to `original-conf-path(FS, clustername)`; this is patched for the
specific instance bindings and saved into `generated-conf-path(FS, clustername)`.

A cluster *exists* if all of these paths are found:

    def cluster-exists(FS, clustername) =
        is-dir(FS, cluster-path(FS, clustername))
        and is-file(FS, cluster-json-path(FS, clustername))
        and is-dir(FS, original-conf-path(FS, clustername))
        and generated-conf-path(FS, original-conf-path(FS, clustername))

A cluster is considered `running` if there is a Hoya application type belonging to the current user in one of the states
`{NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING}`. 

    def final-yarn-states = {FINISHED, FAILED, KILLED }


    def hoya-app-running-instances(YARN, clustername, user) =
        [a in user-applications(YARN, "hoya", user) where:
             and a.Name == clustername
             and not a.YarnApplicationState in final-yarn-state]


    def hoya-app-running(YARN, clustername, user) =
        [] != hoya-app-running-instances(YARN, clustername, user) 

### Invariant: there must never be more than one running instance of a named Hoya cluster


There must never be more than one instance of the same Hoya cluster running:

    forall a in user-applications(YARN, "hoya", user):
        len(hoya-app-running(YARN, a.Name, user)) <= 1
        
There may be multiple instances in a finished state, and one running instance alongside multiple finished instances -the applications
that work with Hoya MUST select a running cluster ahead of any terminated clusters.

### Valid Cluster JSON Cluster Description

The `cluster.json` file of a cluster configures Hoya to deploy the application. 

#### well-defined-cluster(cluster-description)

A Cluster Description is well-defined if it is valid JSON and required properties are present

Irrespective of specific details for deploying the Hoya AM or any provider-specific role instances,
a Cluster Description defined in a `cluster.json` file at the path `cluster-json-path(FS, clustername)`
is well-defined if

1. It is parseable by the jackson JSON parser.
1. Root elements required of a Hoya cluster specification must be defined, and, where appropriate, non-empty
1. It contains the extensible elements required of a Hoya cluster specification. For example, `options` and `roles`
1. The types of the extensible elements match those expected by Hoya.
1. The `version` element matches a supported version
1. Exactly one of `options/cluster.application.home` and `options/cluster.application.image.path` must exist.
1. Any cluster options that are required to be integers must be integers

This specification is very vague here to avoid duplication: the cluster description structure is currently implicitly defined in 
`org.apache.hadoop.hoya.api.ClusterDescription` 

Currently Hoya ignores unknown elements during parsing. This may be changed.

The test for this state does not refer to the cluster filesystem



#### deployable-cluster(FS, cluster-description)

A  Cluster Description defines a deployable cluster if it is well-defined cluster and the contents contain valid information to deploy a cluster

This defines how a cluster description is valid in the extends the valid configuration with 

1. The entry `name` must match a supported provider
1. Any elements that name the cluster match the cluster name as defined by the path to the cluster:
  
    originConfigurationPath == original-conf-path(FS, clustername)
    generatedConfigurationPath == generated-conf-path(FS, clustername)
    dataPath == data-path(FS, clustername)

1. The paths defined in `originConfigurationPath` , `generatedConfigurationPath` and `dataPath` must all exist.
1. `options/zookeeper.path` must be defined and refer to a path in the ZK cluster
defined by (`options/zookeeper.hosts`, `zookeeper.port)` to which the user has write access (required by HBase and Accumulo)
1. If `options/cluster.application.image.path` is defined, it must exist and be readable by the user.
1. Providers may preform their own preflight validation.


#### valid-for-provider(cluster-description, provider)

A provider considers a specification valid if its own validation logic is satisfied. This normally
consists of rules about the number of instances of different roles; it may include other logic.

