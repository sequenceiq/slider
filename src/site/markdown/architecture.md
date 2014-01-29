<!---
~~ Licensed under the Apache License, Version 2.0 (the "License");
~~ you may not use this file except in compliance with the License.
~~ You may obtain a copy of the License at
~~
~~   http://www.apache.org/licenses/LICENSE-2.0
~~
~~ Unless required by applicable law or agreed to in writing, software
~~ distributed under the License is distributed on an "AS IS" BASIS,
~~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
~~ See the License for the specific language governing permissions and
~~ limitations under the License. See accompanying LICENSE file.
-->

# Architecture

## Summary

Hoya is a YARN application to deploy non-YARN-enabled applications in a YARN cluster

Hoya consists of a YARN application master, the "hoya AM", and a client application which communicates with YARN and the Hoya AM via remote procedure calls. The client application offers command line access, as well as low-level API access for test purposes

The deployed application must be a program that can be run across a pool of
YARN-managed servers, dynamically locating its peers. It is not Hoya's
responsibility to configure up the peer servers, apart from some initial
application-specific cluster configuration. (The full requirements
of an application are [described in another document](app_needs.md).

Every cluster application is described as a set of one or more *roles*; each
role can have a different program/command, and a different set of configuration
options and parameters.

The AM takes the details on which roles to start, and requests a YARN container
for each role; It then monitors the state of the cluster, receiving messages
from YARN when a remotely executed process finishes. It then deploys another instance of 
that role.

## Hoya Cluster Provider

A provider sets up the Hoya cluster:
 
1. validating the create time input
1. helping build the initial specification by defining the template roles
1. preflight checking -client side- of parameters before creating a cluster. (these must also be done in the AM; client-side allows for fail-fast checking with better error messages, as well as testability.
1. client-side addition of extra files and options to the AM startup. For example,
   adding the HBase tar file as a resource, and HBase JARs on the classpath.
1. In the AM, setting up the launchers for the various roles.
1. In the AM, helping monitor the state of launched role instances. (Once liveness monitoring is implemented)

## AM Architecture

The AM always has the role "hoya". It is a YARN service, following the YARN lifecycle.

The application master consists of

 1. The AM engine which handles all integration with external services, specifically YARN and any Hoya clients
 1. A provider specific to the desired cluster type.
 1. The Application State. 

The Application State is the model of the cluster, containing

 1. A specification of the desired state of the cluster -the number of instances of each role, their YARN and process memory requirements and some other options. 
 1. A map of the current instances of each role across the cluster, including reliability statistics of each node in the cluster used.
 1. [The Role History](rolehistory.html) -a record of which nodes roles were deployed on for re-requesting the same nodes in future. This is persisted to disk and re-read if present, for faster cluster startup times lists to track outstanding requests, released and starting nodes

The Application Engine integrates with the outside world: the YARN Resource Manager ("the RM"), and the node-specific Node Managers, receiving events from the services, requesting or releasing containers via the RM,  and starting applications on assigned containers.

After any notification of a change in the state of the cluster (or an update to the client-supplied cluster specification), the Application Engine passes the information on to the Application State class, which updates its state and then returns a list of cluster operations to be submitted: requests for containers of different types -potentially on specified nodes, or requests to release containers.

As those requests are met and allocation messages passed to the Application Engine, it works with the Application State to assign them to specific roles, then invokes the provider to build up the launch context for that application.

The provider has the task of populating  container requests with the file references, environment variables and commands needed to start the provider's supported programs. For example, the HBase provider issues commands "bin/hbase master" and "bin/hbase worker" in a directory which will exist after a specified hbase.tar file is downloaded from HDFS and expanded in the target node. 

The clean model-view split was done to isolate the model and aid mock testing of large clusters with simulated scale, and hence increase confidence that Hoya can scale to work in large YARN clusters and with larger application instances. 

To summarize: Hoya is not an classic YARN analysis application, which allocates and schedules work across the cluster in short-to-medium life containers with the lifespan of a query or an analytics session, but instead for an application with a lifespan of days to months. Hoya works to keep the actual state of its application cluster to match the desired state, while the application has the tasks of recovering from node failure, locating peer nodes and working with data in an HDFS filesystem. 

As such it is one of the first applications designed to use YARN as a platform for long-lived services -Samza being the other key example. These application's  needs of YARN are different, and their application manager design is focused around maintaining the distributed application in its desired state rather than the ongoing progress of submitted work.


### Failure Model

The application master is designed to be a [crash-only application](https://www.usenix.org/legacy/events/hotos03/tech/full_papers/candea/candea.pdf), clients are free to terminate
the cluster by asking YARN directly. 

There is an RPC call to stop the cluster - this is a nicety which includes a message in the termination log, and
could, in future, perhaps warn the provider that the cluster is being torn down. That is a potentially dangerous feature
to add -as provider implementors may start to expect the method to be called reliably. Hoya is designed to fail without
warning, to rebuild its state on a YARN-initiated restart, and to be manually terminated without any advance notice.

### RPC Interface


The RPC interface allows the client to query the current application state, and to update it by pushing out a new JSON specification. 

The core operations are

* `getJSONClusterStatus()`: get the status of the cluster as a JSON document.
* `flexCluster()` update the desired count of role instances in the running cluster.
* `stopCluster` stop the cluster

There are some other low-level operations for extra diagnostics and testing, but they are of limited importancs 

The JSON cluster status document returned via `getJSONClusterStatus()` is the same format as used for the persistent cluster description, and is visible
on the command line via the `status` action. It extends the persistent state with details on the containers in use,
and some statistics on the operational state of the cluster.

The `flexCluster()` call takes a JSON cluster specification and forwards it to the AM -which extracts the desired counts of each role to update the Application State. A change in the desired size of the cluster, is treated as any reported failure of node:
it triggers a re-evaluation of the cluster state, building up the list of container add and release requests to make of
the YARN resource manager.

The final operation, `stopCluster()`, stops the cluster. 

### Security and Identity

Hoya's security model is described in detail in [an accompanying document](security.html)

A Hoya cluster is expected to access data belonging to the user creating the cluster. 

In a secure cluster, this is done by acquiring Kerberos tokens in the client when the cluster is updated, tokens which
are propagated to the Hoya AM and thence to the deployed application containers themselves. These
tokens are valid for a finite time period. 

HBase has always required keytab files to be installed on every node in cluster for it to have secure access -this requirement
holds for Hoya-deployed HBase clusters. Hoya does not itself adopt the responsibility of preparing or distributing these files;
this must be done via another channel.

In Hadoop 2.2, the tokens for communication between the Hoya AM and YARN expire after -by default- 72 hours. The
HDFS tokens will also expire after some time period. This places an upper bound on the lifespan of a Hoya application (or any
other long-lived YARN application) in a secure YARN cluster. 



In an insecure cluster, the Hoya AM and its containers are likely to run in a different OS account from the submitting user.
To enable access to the database files as that submitting use, the identity of the user is provided when the AM is created; the
AM will pass this same identity down to the created containers. This information *identifies* the user -but does not *authenticate* them: they are trusted to be who they claim to be.

 
