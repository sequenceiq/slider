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
  
# Specification of the "Cluster Description"

# Version 2.0.0

### Notation: 

In this document, a full path to a value is represented as a path 
`options/zookeeper.port`  ; an assigment as  `options/zookeeper.port=2181`.

A wildcard indicates all entries matching a path: `options/zookeeper.*`
or `/roles/*/yarn.memory`


## Core Concepts

A persistent cluster is defined in a "Cluster Description" JSON file,
saved into HDFS as `cluster.json` in a standarized location for each
cluster (by default, `${user.home}/.hoya/clusters/${clustername}/cluster.json`)

An extended form of the same JSON specification is used to provide status
information back from the Application Master, "AM", to client applications.


## Sections for specifying and describing cluster state

The cluster desciption is hierarchal, with standardized sections.

Different sections have one of three roles.

1. Storage and specification of internal properties used to define a cluster -properties
that should not be modified by users -doing so is likely to render the
cluster undeployable.

1. Storage and specification of the components deployed by Hoya.
These sections define options for the deployed application, the size of
the deployed application, attributes of the deployed roles, and customizable
aspects of the Hoya application master. 

  This information defines the *desired state* of a cluster.
   
  Users may edit these sections, either via the CLI, or by directly editing the `cluster.json` file of
  a frozen cluster.

1. Status information provided by a running cluster. These include:
 information about the cluster, statistics, information about reach role in
 the cluster -as well as other aspects of the deployment.
 
 This information describes the *actual state* of a cluster.
  
Using a common format for both the specification and description of a cluster
may be confusing, but it is designed to unify the logic needed to parse
and process cluster descriptions. There is only one JSON file to parse
-merely different sections of relevance at different times.

## Role-by-role subsections

A hoya-deployed application consists of the single Hoya application master,
and one or more roles -specific components in the actual application.

The `/roles` section contains a listing for each role, 
declaring the number of instances of each role desired,
possibly along with some details defining the actual execution of the application.

The `/statistics/roles/` section returns statistics on each role,
while `/instances` has a per-role entry listing the YARN
containers hosting instances. 


## Cluster information for applications

The AM/application provider may generate information for use



# Specification Sections

## "/" : root

The root contains a limited number of key-value pairs, 

* `version`: string; required.
The version of the JSON file, as an `x.y.z` version string.
    1. Applications MUST verify that they can process a specific version.
    1. The version number SHOULD be incremented in the minor "z" value
    after enhancements that are considered backwards compatible.
    Incompatible updates MUST be updated with a new "y" value.
    The final, "x" number, is to be reserved for major reworkings
    of the cluster specification itself (this document or its
    successors).

* `name`: string; required. Cluster name; 
* `type`: string; required.
Reference to the provider type -this triggers a Hadoop configuration
property lookup to find the implementation classes.
* `valid`: boolean; required.
Flag to indicate whether or not a specification is considered valid.
If false, the rest of the document is in an unknown state.

## `/hoya-internal`

Stores internal configuration options. These parameters
are not defined in this document.

## `/diagnostics`

Persisted list of information about Hoya. 

Static information about the file history
 
    "diagnostics" : {
      "create.hadoop.deployed.info" : "(detached from release-2.3.0) @dfe46336fbc6a044bc124392ec06b85",
      "create.application.build.info" : "Hoya Core-0.13.0-SNAPSHOT Built against commit# 1a94ee4aa1 on Java 1.7.0_45 by stevel",
      "create.hadoop.build.info" : "2.3.0",
      "create.time.millis" : "1393512091276",
    },
 
This information is not intended to provide anything other
than diagnostics to an application; the values and their meaning
are not defined. All applications MUST be able to process
an empty or absent `/diagnostics` section.

## Options

A Persisted list of options used by Hoya and its providers to build up the AM
and the configurations of the deployed service components


  "options": {
    "hoya.am.monitoring.enabled": "false",
    "hoya.cluster.application.image.path": "hdfs://sandbox.hortonworks.com:8020/hbase.tar.gz",
    "hoya.container.failure.threshold": "5",
    "hoya.container.failure.shortlife": "60",
    "zookeeper.port": "2181",
    "zookeeper.path": "/yarnapps_hoya_stevel_test_cluster_lifecycle",
    "zookeeper.hosts": "sandbox",
    "site.hbase.master.startup.retainassign": "true",
    "site.fs.defaultFS": "hdfs://sandbox.hortonworks.com:8020",
    "site.fs.default.name": "hdfs://sandbox.hortonworks.com:8020",
    "env.MALLOC_ARENA_MAX": "4",
    "site.hbase.master.info.port": "0",
    "site.hbase.regionserver.info.port": "0"
  },

Many of the properties are automatically set by Hoya when a cluster is constructed.
They may be edited afterwards.


### Standard Option types

All option values MUST be strings.

#### `hoya.`
All options that begin with `hoya.` are intended for use by hoya and 
providers to configure the Hoya application master itself, and the
application. For example, `hoya.container.failure.threshold` defines
the number of times a container must fail before the role (and hence the cluster)
is considered to have failed. As another example, the zookeeper bindings
such as `zookeeper.hosts` are read by the HBase and Ambari providers, and
used to modify the applications' site configurations with application-specific
properties.

#### `site.`
 
These are properties that are expected to be propagated to an application's
 `site` configuration -if such a configuration is created. For HBase, the 
 site file is `hbase-site.xml`; for Accumulo it is `accumulo-site.xml`

1. The destination property is taken by removing the prefix `site.`, and
setting the shortened key with the defined value.
1. Not all applications have the notion of a site file; These applications MAY
ignore the settings.
1. Providers MAY validate site settings to recognise invalid values. This
aids identifying and diagnosing startup problems.

#### `env.`

These are options to configure environment variables in the roles. When
a container is started, all `env.` options have the prefix removed, and
are then set as environment variables in the target context.

1. The Hoya AM uses these values to configure itself, after following the
option/role merge process.
1. Application providers SHOULD follow the same process.


## '/roles': Role definitions

The `/roles/$ROLENAME/` clauses each provide options for a
specific role.

This includes
1. `role.instances`: defines the number of instances of a role to create
1. `env.` environment variables for launching the container
1. `yarn.` properties to configure YARN requests.
1. `jvm.heapsize`: an option supported by some providers to 
fix the heap size of a component.


      "worker": {
        "yarn.memory": "768",
        "env.MALLOC_ARENA_MAX": "4",
        "role.instances": "0",
        "role.name": "worker",
        "role.failed.starting.instances": "0",
        "jvm.heapsize": "512M",
        "yarn.vcores": "1",
      },


The role `hoya` represents the Hoya Application Master itself.

      
      "hoya": {
        "yarn.memory": "256",
        "env.MALLOC_ARENA_MAX": "4",
        "role.instances": "1",
        "role.name": "hoya",
        "jvm.heapsize": "256M",
        "yarn.vcores": "1",
      },

## How `/options` and role options are merged.

The options declared for a specific role are merged with the cluster-wide options
to define the final options for a role. This is implemented in a simple
override model: role-specific options can override any site-wide options.

1. The options defined in `/options` are used to create the initial option
map for each role.
1. The role's options are then applied to the map -this may overwrite definitions
from the `/options` section.
1. There is no way to "undefine" a cluster option, merely overwrite it. 
1. The merged map is then used by the provider to create the component.
1. The special `hoya` role is used in the CLI to define the attributes of the AM.

Options set on a role do not affect any site-wide options: they
are specific to the invidual role being created. 

As such, overwriting a `site.` option may have no effect -or it it may
change the value of a site configuration document *in that specific role instance*.

*Standard role options*

* `role.instances` : number; required.
  The number of instances of that role desired in the application.
* `yarn.vcores` : number.
  The number of YARN "virtual cores" to request for each role instance.
  The larger the number, the more CPU allocation -and potentially the longer
  time to satisfy the request and so instantiate the node. 
  If the value '"-1"` is used -for any role but `hoya`-the maximum value
  available to the application is requested.
* `yarn.memory` : number.
  The number in Megabytes of RAM to request for each role instance.
  The larger the number, the more memory allocation -and potentially the longer
  time to satisfy the request and so instantiate the node. 
  If the value '"-1"` is used -for any role but `hoya`-the maximum value
  available to the application is requested.
 
* `env.` environment variables.
String environment variables to use when setting up the container

*Provider-specific role options*
  
* `jvm.heapsize` -the amount of memory for a provider to allocate for
 a processes JVM. Example "512M". This option MAY be implemented by a provider.
 




# Information Sections

These are the parts of the document that provide dynamic run-time
information about an application. They are provided by the
Hoya Application Master when a request for the cluster status is issued.

## `/info`

Dynamic set of string key-value pairs containing
information about the running application -as provided by th 

The values in this section are not normatively defined. 

However, here are some 
 
 
* `hoya.am.restart.supported"`  whether the AM supports service restart without killing all the containers hosting
 the role instances:
 
        "hoya.am.restart.supported" : "false",
    
* timestamps of the cluster going live, and when the status query was made
    
        "live.time" : "27 Feb 2014 14:41:56 GMT",
        "live.time.millis" : "1393512116881",
        "status.time" : "27 Feb 2014 14:42:08 GMT",
        "status.time.millis" : "1393512128726",
    
* yarn data provided to the AM
    
        "yarn.vcores" : "32",
        "yarn.memory" : "2048",
      
*  information about the application and hadoop versions in use. Here
  the application was built using Hadoop 2.3.0, but is running against the version
  of Hadoop built for HDP-2.
  
        "status.application.build.info" : "Hoya Core-0.13.0-SNAPSHOT Built against commit# 1a94ee4aa1 on Java 1.7.0_45 by stevel",
        "status.hadoop.build.info" : "2.3.0",
        "status.hadoop.deployed.info" : "bigwheel-m16-2.2.0 @704f1e463ebc4fb89353011407e965"
     
 
As with the `/diagnostics` section, this area is primarily intended
for debugging.

 ## `instances`
 
 Information about the live containers in a cluster

     "instances": {
       "hoya": [ "container_1393511571284_0002_01_000001" ],
       "master": [ "container_1393511571284_0002_01_000003" ],
       "worker": [ 
         "container_1393511571284_0002_01_000002",
         "container_1393511571284_0002_01_000004"
       ]
     },

There's no information about location, nor is there any history about containers
that are no longer part of the cluster (i.e. failed & released containers). 

It could be possible to include a list of previous containers,
though Hoya would need to be selective about how many to store
(or how much detail to retain) on those previous containers.

Perhaps the list could be allowed to grow without limit, but detail
only preserved on the last 100. If more containers fail than that,
there is likely to be a problem which the most recent containers
will also display.
 
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
           "containers.live": 2,
           "containers.start.failed": 0,
           "containers.active.requests": 0,
           "containers.failed": 0,
           "containers.completed": 0,
           "containers.desired": 2,
           "containers.requested": 0
         },
         "hoya": {
           "containers.unknown.completed": 0,
           "containers.start.completed": 3,
           "containers.live": 1,
           "containers.start.failed": 0,
           "containers.failed": 0,
           "containers.completed": 0,
           "containers.surplus": 0
         },
         "master": {
           "containers.start.completed": 0,
           "containers.live": 1,
           "containers.start.failed": 0,
           "containers.active.requests": 0,
           "containers.failed": 0,
           "containers.completed": 0,
           "containers.desired": 1,
           "containers.requested": 0
         }
       },
    
The `/statistics/hoya` section is unusual in that it provides the aggregate statistics
of the cluster -this is not obvious. A different name could be used -but
again, there's a risk of clash with or confusion with a role. 

Better to have a specific `/statistics/cluster` element, 
and to move the roles' statistics under `/statistics/roles`:

    "statistics": {
      "cluster": {
        "containers.unknown.completed": 0,
        "containers.start.completed": 3,
        "containers.live": 1,
        "containers.start.failed": 0,
        "containers.failed": 0,
        "containers.completed": 0,
        "containers.surplus": 0
  
      },
      "roles": {
        "worker": {
          "containers.start.completed": 0,
          "containers.live": 2,
          "containers.start.failed": 0,
          "containers.active.requests": 0,
          "containers.failed": 0,
          "containers.completed": 0,
          "containers.desired": 2,
          "containers.requested": 0
        },
        "master": {
          "containers.start.completed": 0,
          "containers.live": 1,
          "containers.start.failed": 0,
          "containers.active.requests": 0,
          "containers.failed": 0,
          "containers.completed": 0,
          "containers.desired": 1,
          "containers.requested": 0
        }
      }
    },

This approach allows extra statistics sections to be added (perhaps
by providers), without any changes to the toplevel section.


### Proposed:  `/clientProperties` continues return Key-val pairs

The `/clientProperties` section will remain, with key-val pairs of type
string, the expectation being this is where providers can insert specific
single attributes for client applications.

These values can be converted to application-specific files on the client,
in code -as done today in the Hoya CLI-, or via template expansion (beyond
the scope of this document.



### Proposed: alongside `/clientProperties`  comes `/clientfiles` 

This section will list all files that an application instance can generate
for clients, along with with a description.

    "/clientfiles/hbase-site.xml": "site information for HBase"
    "/clientfiles/log4.properties": "log4.property file"

A new CLI command would be added to retrieve a client file.
1. The specific file must be named.
1. If it is not present, an error must be raised.
1. If it is present, it is downloaded and output to the console/to a named
destination file/directory `--outfile <file>` and `--outdir <dir>`
1. If the `--list` argument is provided, the list of available files is
returned (e.g.) 

    hbase-site.xml: site information for HBase
    log4.properties: log4.property file
    
*No attempt to parse/process the body of the messages will be returned.*

In a REST implementation of the client API, /clientconf would be a path
to the list of options; each file a path underneath.

Client configuration file retrieval outside the status completely;
the status just lists the possible values; a separate call returns them.

This will  permit binary content to be retrieved, and avoid any marshalling
problems and inefficiencies.

With this change, there will now be two ways to generate client configuration
files

* Client-side: as today
* Server-side: via the provider

Client side is more extensible as it allows for arbitrary clients; server-side
is restricted to those files which the application provider is capable of
generating. The advantage of the server-side option is that for those files
about which the provider is aware of, they will be visible through the 
REST and Web UIs, so trivially retrieved.

### Stop intermixing role specification with role current state

Create a new section, `rolestatus`, which lists the current status
of the roles: how many are running vs requested, how many are being
released.

There's some overlap here with the `/statistics` field, so we should
either merge them or clearly separate the two. Only the `role.failed`
properties match entries in the statistics -perhaps they should be cut.

#### provider-specific status

Allow providers to publish information to the status, in their
own section.

There already is support for providers updating the cluster status
in Hoya 12.1 and earlier, but it has flaws

A key one is that it is done sychronously on a `getStatus()` call;
as providers may perform a live query of their status (example, the HBase
provider looks up the Web UI ports published by HBase to zookeeper),
there's overhead, and if the operation blocks (example: when HBase hasn't
ever been deployed and the zookeeper path is empty), then the status
call blocks.

*Proposed:*

1. There is a specific `/provider` section
1. There's no restriction on what JSON is permitted in this section.
1. Providers may make their own updates to the application state to read and
write this block -operations that are asynchronous to any status queries.