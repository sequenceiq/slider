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

Hoya is a YARN application whose Application Master (the "Hoya AM"), sets up
the actual cluster application for YARN to deploy and run. 

The cluster application must be a program that can be run across a pool of
YARN-managed servers, dynamically locating its peers. It is not Hoya's
responsibility to join up the peer servers, apart from some initial
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
1. preflight checking -client side- of parameters before creating a cluster.
(these must also be done in the AM; client-side allows for fail-fast checking
with better error messages, as well as testability.
1. client-side addition of extra files & options to the AM startup. For example,
adding the HBase tar file as a resource, and HBase JARs on the classpath.
1. In the AM, setting up the launchers for the various roles.
1. In the AM, helping monitor the state of launched role instances. (Once liveness monitoring is implemented)

## AM Architecture

The AM always has the role "master". It is a Yarn service, following the YARN lifecycle.

It supports the addition of sub-services supplied by the provider; these are expected
(but not always) processes.

