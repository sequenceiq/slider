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
  
# CLI Actions
 
## client configuration
 
As well as the CLI options, the `conf/hoya-client.xml` XML file can define arguments used to communicate with the Hoya cluster


####    `fs.defaultFS`

Equivalent to setting the filesystem with `--filesystem`

####   `hoya.security.enabled`

Equivalent to `--secure`


## Common

### System Properties

Arguments of the form `-S key=value` define JVM system properties.

These are supported primarily to define options needed for some Kerberos configurations.

### Definitions
 
Arguments of the form `-D key=value` define JVM system properties.

These can define client options that are not set in `conf/hoya-client.xml` - or to override them.
 
 
## Action: Build

Builds a cluster -creates all the on-filesystem datastructures, and generates a cluster description
that is both well-defined and deployable -*but does not actually start the cluster*

    build (clustername,
       roleOptions:Map[String,Map[String:String]],
       options:Map[String:String],
       options:Map[String:String],
       confdir: URI,
      ZK-quorum,
      zkport,
      resource-manager?, 


#### Preconditions

Note that the ordering of these preconditions is not guaranteed to remain constant.

The cluster name is valid

    if not valid-cluster-name(clustername) : raise HoyaException(EXIT_COMMAND_ARGUMENT_ERROR)

The cluster must not be live. This is purely a safety check as the next test should have the same effect.

    if hoya-app-running(YARN, clustername) : raise HoyaException(EXIT_CLUSTER_IN_USE)

The cluster must not exist

    if is-dir(HDFS, cluster-path(FS, clustername)) : raise HoyaException(EXIT_CLUSTER_EXISTS)

The configuration directory must exist in the referenced filesystem (which does not have to the cluster's HDFS instance),
and must contain only files

    let FS = FileSystem.get(confdir)
    if not isDir(FS, confdir) raise HoyaException(EXIT_COMMAND_ARGUMENT_ERROR)
    forall f in children(FS, confdir) :
        if not isFile(f): raise IOException

There's a race condition at build time where between the preconditions being met and the cluster specification being saved, the cluster
is created by another process. The strategy to address this is 

1. Verify that the cluster directory does not exist.
1. Perform any validation needed on the arguments and build up the Cluster Description instance
1. Save that to `cluster-json-path(FS, clustername)` -using `create(FS, path, false)` to fail the operation
if the cluster JSON file already exists. Provided the filesystem atomically checks and creates the JSON file,
this will reject the second attempt to build the cluster. HDFS does meet that requirement.


#### Postconditions

All the cluster directories exist

    is-dir(HDFS, cluster-path(HDFS, clustername))
    is-dir(HDFS, original-conf-path(HDFS, clustername))
    is-dir(HDFS, generated-conf-path(HDFS, clustername))
    is-dir(HDFS, data-path(HDFS, clustername))

The cluster specification saved is well-defined and deployable

    let cluster-description = FS.read(HDFS.open(cluster-json-path(FS, clustername))
    well-defined-cluster(cluster-description)
    deployable-cluster(HDFS, cluster-description)

More precisely: the specification generated before it is saved as JSON is well-defined and deployable; no JSON file will be created
if the validation fails.


Fields in the cluster description have been filled in

    cluster-description["createTime"] > 0
    cluster-description["createTime"] <= System.currentTimeMillis()
    cluster-description["state"]  = ClusterDescription.STATE_CREATED;
    cluster-description["options"]["zookeeper.port"]  = zkport
    cluster-description["options"]["zookeeper.hosts"]  = zkhosts


All files that were in the configuration directory are now copied into the "original" configuration directory

    let FS = FileSystem.get(confdir)
    let dest = original-conf-path(HDFS, clustername)
    if not isDir(FS, confdir) raise FileNotFoundException
    forall [c in children(FS, confdir) :
        data(HDFS, dest + [filename(c)]) == data(FS, c)

All files that were in the configuration directory now have equivalents in the generated configuration directory

    let FS = FileSystem.get(confdir)
    let dest = original-conf-path(HDFS, clustername)
    if not isDir(FS, confdir) raise FileNotFoundException
    forall [c in children(FS, confdir) :
        isfile(HDFS, dest + [filename(c)])
