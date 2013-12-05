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
 
### Cluster names

All actions that must take a cluster name will fail with `EXIT_UNKNOWN_HOYA_CLUSTER`
if one is not provided.

## Action: Build

Builds a cluster -creates all the on-filesystem datastructures, and generates a cluster description
that is both well-defined and deployable -*but does not actually start the cluster*

    build (clustername,
      roleSizes:List[(String, int)],
      roleOptions:List[(String,String, String)],
      options:List[(String,String)],
      confdir: URI,
      provider: String
      zkhosts,
      zkport,
      image
      apphome
      appconfdir
      

#### Preconditions

(Note that the ordering of these preconditions is not guaranteed to remain constant)

The cluster name is valid

    if not valid-cluster-name(clustername) : raise HoyaException(EXIT_COMMAND_ARGUMENT_ERROR)

The cluster must not be live. This is purely a safety check as the next test should have the same effect.

    if hoya-app-running(YARN, clustername) : raise HoyaException(EXIT_CLUSTER_IN_USE)

The cluster must not exist

    if is-dir(HDFS, cluster-path(FS, clustername)) : raise HoyaException(EXIT_CLUSTER_EXISTS)

The configuration directory must exist it does not have to be the cluster's HDFS instance,
as it will be copied there -and must contain only files

    let FS = FileSystem.get(appconfdir)
    if not isDir(FS, appconfdir) raise HoyaException(EXIT_COMMAND_ARGUMENT_ERROR)
    forall f in children(FS, appconfdir) :
        if not isFile(f): raise IOException

There's a race condition at build time where between the preconditions being met and the cluster specification being saved, the cluster
is created by another process. The strategy to address this is:

1. Verify that the cluster directory does not exist.
1. Perform any validation needed on the arguments and build up the Cluster Description instance
1. Save that to `cluster-json-path(FS, clustername)` -using `create(FS, path, false)` to fail the operation
if the cluster JSON file already exists. Provided the filesystem atomically checks and creates the JSON file,
this will reject the second attempt to build the cluster. HDFS does meet that requirement.


#### Postconditions

All the cluster directories exist

    is-dir(HDFS', cluster-path(HDFS', clustername))
    is-dir(HDFS', original-conf-path(HDFS', clustername))
    is-dir(HDFS', generated-conf-path(HDFS', clustername))

The cluster specification saved is well-defined and deployable

    let cluster-description = parse(data(HDFS', cluster-json-path(HDFS', clustername)))
    well-defined-cluster(cluster-description)
    deployable-cluster(HDFS', cluster-description)

More precisely: the specification generated before it is saved as JSON is well-defined and deployable; no JSON file will be created
if the validation fails.

Fields in the cluster description have been filled in

    clusterspec["type"] == provider
    clusterspec["createTime"] > 0
    clusterspec["createTime"] <= System.currentTimeMillis()
    clusterspec["state"]  == ClusterDescription.STATE_CREATED;
    clusterspec["options"]["zookeeper.port"]  == zkport
    clusterspec["options"]["zookeeper.hosts"]  == zkhosts

Any `apphome` and `image` properties have propagated

    apphome == null or clusterspec.options["cluster.application.home"] == apphome
    image == null or clusterspec.options["cluster.application.image.path"] == image

(The `well-defined-cluster()` requirement above defines the valid states
of this pair of options)


All role sizes have been mapped to `role.instances` fields

    forall (name, size) in roleSizes :
        clusterspec.roles[name]["role.instances"] == size


All role option parameters have been added to the specific role's option map

      forall (name, opt, val) in roleOptions :
          clusterspec.roles[name][opt] == val

All option parameters have been added to the `options` map in the specification

      forall (opt, val) in options :
          clusterspec.options[opt] == val

There's no explicit rejection of duplicate options, the outcome of that
state is 'undefined'. 

What is defined is that if Hoya or its provider provided a default option value,
the command-line supplied option will override it.

All files that were in the configuration directory are now copied into the "original" configuration directory

    let FS = FileSystem.get(appconfdir)
    let dest = original-conf-path(HDFS', clustername)
    forall [c in children(FS, confdir) :
        data(HDFS', dest + [filename(c)]) == data(FS, c)

All files that were in the configuration directory now have equivalents in the generated configuration directory

    let FS = FileSystem.get(appconfdir)
    let dest = generated-conf-path(HDFS', clustername)
    forall [c in children(FS, confdir) :
        isfile(HDFS', dest + [filename(c)])


## Action: Thaw

    thaw clustername [--wait [timeout]]

Thaw takes a cluster with configuration and (possibly) data on disk, and
attempts to instantiate a Hoya cluster with the specified number of nodes

#### Preconditions

    if not valid-cluster-name(clustername) : raise HoyaException(EXIT_COMMAND_ARGUMENT_ERROR)

The cluster must not be live. This is purely a safety check as the next test should have the same effect.

    if hoya-app-running(YARN, clustername) : raise HoyaException(EXIT_CLUSTER_IN_USE)

The cluster must not exist

    if is-dir(HDFS, cluster-path(FS, clustername)) : raise HoyaException(EXIT_CLUSTER_EXISTS)

The cluster specification must exist, be valid and deployable

    if not is-file(HDFS, cluster-json-path(HDFS, clustername)) : HoyaException(EXIT_UNKNOWN_HOYA_CLUSTER)
    if not well-defined-cluster(HDFS, cluster-path(HDFS, clustername)) : raise HoyaException(EXIT_BAD_CLUSTER_STATE)
    if not deployable-cluster(HDFS, cluster-path(HDFS, clustername)) : raise HoyaException(EXIT_BAD_CLUSTER_STATE)

### Postconditions


After the thaw has been performed, there is now a queued request in YARN
for the chosen (how?) queue

    YARN'.Queues'[amqueue] = YARN.Queues[amqueue] + [launch("hoya", clustername, requirements, context)]

If a wait timeout was specified, the cli waits until the application is considered
running by YARN (the AM is running), the wait timeout has been reached, or
the application has failed

    waittime < 0 or (exists a in hoya-app-running-instances(hoya-app-instances(YARN', clustername, user))
        where a.YarnApplicationState == RUNNING)


## Outcome: AM-launched state

Some time after the AM was queued, if the relevant
prerequisites of the launch request are met, the AM will be deployed

#### Preconditions

* The resources referenced in HDFS (still) are accessible by the user
* The requested YARN memory and core requirements could be met on the cluster and queue.
* There is sufficient capacity in the cluster to create a container for the AM.

#### Postconditions

Define a YARN state at a specific time `t` as `YARN(t)`; the fact that
an AM is launched afterwards

The AM is deployed if there is some time `t` after the submission time `t0`
where the application is listed 

    exists t1 where t1 > t0 and hoya-app-live(YARN(t1), user, clustername)

At which time there is a container in the cluster hosting the AM -it's
context is the launch context

    exists c in containers(YARN(t1)) where container.context = launch.context

There's no way to determine when this time `t1` will be reached -or if it ever
will -its launch may be postponed due to a lack of resources and/or higher priority
requests using resources as they become available.

For tests on a dedicated cluster, a few tens of seconds appear to be enough
for the AM-launched state to be reached, a failure to occur, or to conclude
that the resource requirements are unsatisfiable.

## Outcome: AM-started state

A (usually short) time after the AM is launched, it should start

* The node hosting the container is working reliably
* The supplied command line could start the process
* the localized resources in the context could be copied to the container (which implies
that they are readable by the user account the AM is running under)
* The combined classpath of YARN, extra JAR files included in the launch context,
and the resources in the hoya client 'conf' dir contain all necessary dependencies
to run Hoya.
* There's no issue with the cluster specification that causes the AM to exit
with an error code.

Node failures/command line failures are treated by YARN as an AM failure which
will trigger a restart attempt -this may be on the same or a different node.

#### preconditions

The AM was launched at an earlier time, `t1`

    exists t1 where t1 > t0 and am-launched(YARN(t1)


#### Postconditions

The application is actually started if it is listed in the YARN application list
as being in the state `RUNNING`, an RPC port has been registered with YARN (visible as the `rpcPort`
attribute in the YARN Application Report,and that port is servicing RPC requests
from authenticated callers.

    exists t2 where:
        t2 > t1 
        and hoya-app-live(YARN(t2), YARN, clustername, user)
        and hoya-app-live-instances(YARN(t2))[0].rpcPort != 0
        and rpc-connection(hoya-app-live-instances(YARN(t2))[0], HoyaClusterProtocol)

A test for accepting cluster requests is querying the cluster status
with `HoyaClusterProtocol.getJSONClusterStatus()`. If this returns
a parseable cluster description, the AM considers itself live.

## Outcome: Hoya operational state

Once started, Hoya enters the operational state of trying to keep the numbers
of live role instances matching the numbers specified in the cluster specification.

The AM must request the a container for each desired instance of a specific roles of the
application, wait for those requests to be granted, and then instantiate
the specific application roles on the allocated containers.

Such a request is made on startup, whenever a failure occurs, or when the
cluster size is dynamically updated.

The AM releases containers when the cluster size is shrunk during a flex operation,
or during teardown.

### steady state condition

The steady state of a Hoya cluster is that the number of live instances of a role,
plus the number of requested instances , minus the number of instances for
which release requests have been made must match that of the desired number.

If the internal state of the Hoya AM is defined as `AppState`

    forall r in clusterspec.roles :
        r["role.instances"] ==
          AppState.Roles[r].live + AppState.Roles[r].requested - AppState.Roles[r].released

The `AppState` represents Hoya's view of the external YARN system state, based on its
history of notifications received from YARN. 

It is indirectly observable from the cluster state which an AM can be queried for


    forall r in AM.getJSONClusterStatus().roles :
        r["role.instances"] ==
          r["role.actual.instances"] + r["role.requested.instances"] - r["role.releasing.instances"]

Hoya does not consider it an error if the number of actual instances remains below
the desired value (i.e. outstanding requests are not being satisfied) -this is
an operational state of the cluster that Hoya cannot address.

### Cluster startup

On a healthy dedicated test cluster, the time for the requests to be satisfied is
a few tens of seconds at most: a failure to achieve this state is a sign of a problem.

### Node or process failure

After a container or node failure, a new container for a new instance of that role
is requested.

The failure count is incremented -it can be accessed via the `"role.failed.instances"`
attribute of a role in the status report.

The number of failures of a role is tracked, and used by Hoya as to when to
conclude that the role is somehow failing consistently -and it should fail the
entire application.

This has initially been implemented as a simple counter, with the cluster
option: `"hoya.container.failure.threshold"` defining that threshold.

    let status = AM.getJSONClusterStatus() 
    forall r in in status.roles :
        r["role.failed.instances"] < status.options["hoya.container.failure.threshold"]


### Instance startup failure


Startup failures are measured alongside general node failures.

A container is deemed to have failed to start if either of the following conditions
were met:

1. The AM received an `onNodeManagerContainerStartFailed` event.

1. The AM received an `onCompletedNode` event on a node that started less than 
a specified number of seconds earlier -a number given in the cluster option
`"hoya.container.failure.shortlife"`. 

More sophisticated failure handling logic than is currently implemented may treat
startup failures differently from ongoing failures -as they can usually be
treated as a sign that the container is failing to launch the program reliably -
either the generated command line is invalid, or the application is failing
to run/exiting on or nearly immediately.

## Action: Create

Create is simply `build` + `thaw` in sequence  - the postconditions from the first
action are intended to match the preconditions of the second.

## Action: freeze

    freeze clustername [--wait time]

The *freeze* action "freezes" the cluster: all its nodes running in the YARN
cluster are stopped, leaving all the persistent state.

The operation is intended to be idempotent: it is not an error if 
freeze is invoked on an already frozen cluster

#### Preconditions

The cluster name is valid and it matches a known cluster 

    if not valid-cluster-name(clustername) : raise HoyaException(EXIT_COMMAND_ARGUMENT_ERROR)
    
    if not is-file(HDFS, cluster-path(HDFS, clustername)) :
        raise HoyaException(EXIT_UNKNOWN_HOYA_CLUSTER)

#### Postconditions

If the cluster was running, an RPC call has been sent to it `stopCluster(message)`

If the `--wait` argument specified a wait time, then the command will block
until the cluster has finished or the wait time was exceeded. 

The outcome should be the same:

    not hoya-app-running(YARN', clustername)

## Action: flex

Flex the cluster size: add or remove roles. Flexing can be
marked as persistent or not; 

    flex clustername 
    roleSizes:List[(String, int)]

1. if persistent, the JSON cluster specification is updated
1. if the cluster is running, it is given the new cluster specification,
which will change the desired steady-state of the application

#### Preconditions

    if not is-file(HDFS, cluster-json-path(HDFS, clustername)) :
        raise HoyaException(EXIT_UNKNOWN_HOYA_CLUSTER)

#### Postconditions

    let originalSpec = data(HDFS, cluster-json-path(HDFS, clustername))
    
    let updatedSpec = originalspec where:
        forall (name, size) in roleSizes :
            updatedSpec.roles[name]["role.instances"] == size
    if persist:
        data(HDFS', cluster-json-path(HDFS', clustername)) == updatedSpec
    rpc-connection(hoya-app-live-instances(YARN(t2))[0], HoyaClusterProtocol)
    let flexed = rpc-connection(hoya-app-live-instances(YARN(t2))[0], HoyaClusterProtocol).flexClusterupdatedSpec)


#### AM actions on flex

    boolean HoyaAppMaster.flexCluster(ClusterDescription updatedSpec)
  
If the  cluster is in a state where flexing is possible (i.e. it is not in teardown),
then `AppState` is updated with the new desired role counts. The operation will
return once all requests to add or remove role instances have been queued,
and be `True` iff the desired steady state of the cluster has been changed.

#### Preconditions

      well-defined-cluster(HDFS, updatedSpec)
  

#### Postconditions

    forall role in AppState.Roles.keys:
        AppState'.Roles'[role].desiredCount = updatedSpec[roles]["role.instances"]
    result = AppState' != AppState


The flexing may change the desired steady state of the cluster, in which
case the relevant requests will have been queued by the completion of the
action. It is not possible to state whether or when the requests will be
satisfied.

## Action: destroy

Idempotent operation to destroy a frozen cluster -it succeeds if the 
cluster has already been destroyed/is unknown, but not if it is
actually running.

#### Preconditions

    if not valid-cluster-name(clustername) : raise HoyaException(EXIT_COMMAND_ARGUMENT_ERROR)

    if hoya-app-running(YARN, clustername) : raise HoyaException(EXIT_CLUSTER_IN_USE)


#### Postconditions

The cluster directory and all its children do not exist

    not is-dir(HDFS', cluster-path(HDFS', clustername))
  

## Action: status

    status clustername
    
BUG-11364 proposes adding an `--outfile` operation to output this to a named file,
this is to aid testing. Currently it goes to stdout.


#### Preconditions

    if not hoya-app-running(YARN, clustername) : raise HoyaException(EXIT_UNKNOWN_HOYA_CLUSTER)

#### Postconditions

The status of the application has been successfully queried and printed out:

    let status = hoya-app-live-instances(YARN).rpcPort.getJSONClusterStatus()
    status.toString() in STDOUT'

## Action: exists

This probes for a named cluster being in the running state; it is essentially the status
operation with the exit code only returned.

#### Preconditions

    if not hoya-app-running(YARN, clustername) : raise HoyaException(EXIT_UNKNOWN_HOYA_CLUSTER)

#### Postconditions

The operation succeeds if the cluster is running and the RPC call returns the cluster
status.

    let status = hoya-app-live-instances(YARN).rpcPort.getJSONClusterStatus()
 
## Action: getConf

This returns the live client configuration of the cluster -the
site-xml file.

    getconf --format (xml|properties) --outfile [filename]

*We may want to think hard about whether this is needed*

#### Preconditions

    if not hoya-app-running(YARN, clustername) : raise HoyaException(EXIT_UNKNOWN_HOYA_CLUSTER)


#### Postconditions

The operation succeeds if the cluster status can be retrieved and saved to 
the named file/printed to stdout in the format chosen

    let status = hoya-app-live-instances(YARN).rpcPort.getJSONClusterStatus()
    let conf = status.clientProperties
    if format == "xml" : 
        let body = status.clientProperties.asXmlDocument()
    else:
        let body = status.clientProperties.asProperties()
        
    if outfile != "" :
        data(LocalFS', outfile) == body
    else
        body in STDOUT'

## Action: list

    list [clustername]

Lists all clusters of a user, or only the one given

#### Preconditions

If a clustername is specified it must be in YARNs list of active or completed applications
of that user:

    if clustername != "" and [] == hoya-app-instances(YARN, clustername, user) 
        raise HoyaException(EXIT_UNKNOWN_HOYA_CLUSTER)


#### Postconditions

If no clustername was given, all hoya applications of that user are listed,
else only the one running (or one of the finished ones)
  
    if clustername == "" :
        forall a in hoya-app-instances(YARN, user) :
            a.toString() in STDOUT'
    else
       let e = hoya-app-instances(YARN, clustername, user) 
       e.toString() in STDOUT'



## Action: monitor

(This is still a work in progress. The goal is to be able to monitor the 
health of a cluster by blocking while HDFS or accumulo is live).
It may not be supported in Hoya 1.x

#### Preconditions



#### Postconditions



## Action: emergency-force-kill

This is an administration operation added while implementing security in
Hoya: it will force kill an application without issuing an RPC Call -instead
it tells the YARN AM to kill it.

    emergency-force-kill appid


#### Preconditions

    appid == valid application ID or appid == "all"


#### Postconditions

If 'all' was requested, then no applications of the user should be running:

    if appid == "all" :
        not exists a for app in YARN'.Apps'.values where app.report.User = username and app.report.State <= RUNNING

If an appId was provided, that application should be in a finished state

    YARN'.Apps'[appId].report.State >= FINISHED
