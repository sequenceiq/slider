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
  
# = Apache Hoya Proposal =

## == Abstract ==

Apache Hoya is an application to provision and create instances of distributed
applications across an Apache Hadoop cluster running the YARN resource manager.

It's goal is to be "an extensible tool for creating and managing distributed
applications under YARN"


## == Proposal ==

Apache Hoya allows users to deploy distributed applications across a Hadoop
cluster, using the YARN resource manager to place components of these
application , across the cluster. Hoya can monitor the health of these deployed
components, and react to their failure or the loss of the servers on which they
run on by creating new instances of them. Hoya can expand or shrink the size of
the application in response to user commands, and pause and resume an
application.

In this way, Hoya can take a classic "deployed by the ops team on every server"
Hadoop application -such as Apache HBase and Apache Accumulo- and make it a
service in a YARN cluster which a user can instantiate, which can flex in size
based on load, and be frozen and thawed on demand.


## == Background ==

Hoya was written as an extension of the YARN resource framework, a YARN
application to deliver the flexibility and recoverability features of a YARN
application to existing Hadoop applications. The original Groovy-language
prototype targeted HBase, hence the name Hoya: HBase On YARN

"Hoya 0.1" could create an HBase cluster in a YARN cluster, using YARN to copy
over the HBase .tar binaries and configuration files supplied by the end user
and modified by Hoya en route. Hoya provided its own YARN Application Master,
(AM), which spawned the HBase master as child process, and requested and
released containers for HBase worker nodes based on demand.

Since then Hoya has evolved based on the experiences of using the
previous iterations, and a long-term goal of creating and managing distributed applications

 * Added the notion of a Provider, a set of classes containing the code to
   support different applications; factoring out HBase support into one such
   provider.

 * Moved from a simple model of "1 one master, many workers", to one of
   multiple roles; each role implementing a different part of a larger
   distributed application.

 * Added an Accumulo provider, with support for the many different roles:
   masters, tablet servers, monitor, and garbage collector.

 * Moved the HBase master to be just another role, allowing HBase clusters to
   be created with multiple backup master nodes -for better availability.

 * Added (persistent) role placement history so that a restarted cluster can
   request role instances on the same nodes used earlier -this increases data
   locality and so can result in faster startup times.

 * Added initial failure tracking, to track servers that appear to be unreliable,
 and to recognize when so many role instances are failing that the Hoya cluster
 should consider itself failed

In the process the core application code was migrated to Java, support for
secure Hadoop clusters added, and the overall feature set and experience of
using the tool improved.

## == Rationale ==

The Hadoop "stack" has long included applications above the core HDFS and
MapReduce layers, with the column table database, Apache HBase, one of the key
applications. To date, an HBase region server has been expected to have been
deployed on every server in the cluster, managed by the operations team and
their tools as yet another static part of the Hadoop system. While an ideal
model for a single-application Hadoop cluster, this model is inflexible in its
use of cluster resources, and, if a cluster was shared across applications,
made management of the HBase database complex technically and politically.
Resource allocations, HBase versions, security access , downtime -all issues
that would need to be resolved across projects.

It was precisely to address these same problems at the MapReduce that the YARN
scheduler was created for: to move the MapReduce engine from a pre-installed
operations-managed service shared across a cluster, to a **user-level
application** : an applications that individuals could be deployed -with their
own version, resource allocations and extensions. Hadoop 2's YARN scheduler
also had a goal that went beyond a better MapReduce experience: it is designed
to open up the cluster's resources to other applications, such as the Apache-incubating Samza
Storm applications that are written with explicit support for YARN.

The applications that YARN can schedule can stretch beyond analytics
applications, so allowing mixed-use clusters. Which includes the column table
databases and other parts of the Hadoop ecosystem. If YARN can deploy them,
they gain the dynamic deployment, failure recovery and cluster size flexing
opportunities, while eliminating the "single version per cluster" restriction
today.

A rewrite purely for YARN is possible, and it may be something that existing
projects could consider in future, once Hadoop 1.x support is no longer a need.
Until then, they need a way to be scheduled and deployed in a YARN cluster
without being rewritten: Hoya can provide this.

## == Initial Goals ==

 * Donate the (already ASF-licensed) Hoya source code and documentation to the
   Apache Software Foundation.

 * Setup and standardize the open governance of the Apache Hoya project.

 * Build a user and developer community

 * Tie in better with HBase, Accumulo and other projects that can take
   advantage of a YARN cluster -yet which are not ready to be explicitly
   migrated to YARN.

 * Build a system that can be used to migrate more distributed applications
   into YARN clusters.

## == Current Status ==

Hoya is a very young project, six months old, and is not yet in production use.

There is interest in it -mostly from people who want to explore Apache
Accumulo. This shows a key benefit of Hoya: anyone with access to a Hadoop
cluster can use Hoya to deploy the applications that it manages -here Accumulo.
Even if the users go on to have a static Accumulo deployment in production, if
Hoya has helped people get started with the product, it can be considered a
success.

## == Meritocracy ==

The core of the code was originally written by one person: Steve Loughran, who
has long-standing experience in Apache projects, with colleagues with HBase
experience (Ted Yu, Devaraj Das) and Accumulo (Billie Rinaldi, Josh Elser).


## == Community ==

Being a young project there is little community, something github hosting
does little to develop.

Where there is community is in HBase and Accumulo proper: a challenge for Hoya
will be how to build awareness of and use of Hoya in these communities. For
that we have to deliver tangible benefits.

## == Alignment ==
The project is aligned with Apache, from its build process up. It depends on
Apache Hadoop, it currently deploys HBase and Accumulo.

For Hadoop, it, along with Samza, drives the work of supporting long-lived
services in YARN, work listed in the
[YARN-896](https://issues.apache.org/jira/browse/YARN-896) work. While many of
the issues listed under YARN-896 relate to service longevity, there is also the
challenge of having low-latency table lookups co-exist with CPU-and-IO
intensive analytics workloads. This may drive future developments in Hadoop
HDFS itself.

## == Known Risks ==

The biggest risk is getting the critical mass of use needed to build a broad
development team. We don't expect to have or need many full-time developers,
but active engagement from the HBase and Accumulo developers would
significantly aid adoption and governance.

## == Documentation ==

All Hoya documentation is currently in markdown-formatted text files in the
source repository; they will be delivered as part of the initial source
donation.

## == Initial Source ==

All initial source can be found at [https://github.com/hortonworks/hoya](https://github.com/hortonworks/hoya) 

## == Source and IP Submission Plan ==

1. All source will be moved to Apache Infrastructure
1. All outstanding issues in our in-house JIRA infrastructure will be replicated into the Apache JIRA system.

## == External Dependencies ==

Hoya has no external dependencies except for some Java libraries that are
considered ASF-compatible (JUnit, SLF4J), and Apache artifacts : Hadoop, HBase,
Accumulo.

## == Required Resources ==

Mailing Lists
 1. hoya-dev
 1. hoya-commits
 1. hoya-private
 1. git repository

Jenkins builds on x86-Linux, ARM-Linux and Windows hooked up to JIRA

## == Initial Committers ==

 1. Steve Loughran (stevel at a.o)
 1. Billie Rinaldi 
 1. Ted Yu
 1. Josh Elser

## == Sponsors ==

Champion

Nominated Mentors


## == Sponsoring Entity ==

Incubator vs Hadoop PMC?  HBase PMC


