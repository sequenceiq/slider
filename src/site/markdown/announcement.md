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

# Hoya!



In the last few weeks, we have been getting together a prototype, Hoya, running HBase On YARN. This is driven by a few top level use cases that we have been trying to address. Some of them are:

* Be able to create on-demand HBase clusters easily -by and or in apps

* With different versions of HBase potentially (for testing etc.)

* Be able to configure different Hbase instances differently

* For example, different configs for read/write workload instances

* Better isolation

* Run arbitrary co-processors in user’s private cluster

* User will own the data that the hbase daemons create

* MR jobs should find it simple to create (transient) HBase clusters

* For Map-side joins where table data is all in HBase, for example

* Elasticity of clusters for analytic / batch workload processing

* Stop / Suspend / Resume clusters as needed

* Expand / shrink clusters as needed

* Be able to utilize cluster resources better

* Run MR jobs while maintaining HBase’s low latency SLAs

The Hoya tool is a Java tool, and is currently CLI driven. It takes in a cluster specification - in terms of the number of regionservers, the location of HBASE_HOME, the ZooKeeper quorum hosts, the configuration that the new HBase cluster instance should use and so on. The tool persists the information as a JSON document on the HDFS. It also generates the configuration files assuming the passed configuration directory as a base - in particular, the HDFS & ZooKeeper root directories for the new HBase instance has to be generated (note that the underlying HDFS and ZooKeeper are shared by multiple cluster instances). Once the cluster has been started, the cluster can be made to grow or shrink using the Hoya commands. The cluster can also be stopped and later resumed. Hoya implements the functionality through YARN APIs and HBase’s shell scripts. The goal of the prototype was to have minimal code changes and as of this writing, it has required zero code changes in HBase.

The current status as of this writing is that most of the basic functionality is working in real cluster setups. In the immediate term, reconfiguration of existing clusters is something to be addressed. The tool needs production level testing and some solidification in terms of error handling and handling corner cases.

More broadly, the next steps on this work spans across the tool itself and YARN. In Hoya, the front end CLI and related parts is Groovy-based. We are still deciding on whether to stick with Groovy or to switch to Java entirely. The Hoya Application Master spawns the HBase Master in the same node. HBase implements Master HA where one can start multiple masters -they uses Zookeeper to choose one active among all of them and others just wait to become active. We need to see whether that model is still needed or not . YARN will reallocate and start a failed ApplicationMaster, which in Hoya's case, would spawn the HBase Master). Data locality handling can be improved. Resuming a cluster could reuse the earlier nodes on a best effort basis. 

On the YARN side, we've been discussing ways to extend it so that MapReduce and HBase workloads can co-exist in the same cluster. HBase RegionServers are usually heavy-weight processes in many configurations and it might make sense to restrict the number of RegionServers that should be run on any given node to one. Administrators might require that HBase daemons run on specific racks/machines and YARN could provide a way to handle that (apparently, there is a thought that nodes could have labels and applications could ask for nodes with specific labels). Log file(s) handling for long running applications should be looked at in YARN. Policy-based container releases should be looked at (for example, one policy for releasing HBase RegionServer containers could be based on region count - when needed, containers with the lowest number of regions could be released to minimize the churn in the HBase layer).

These are all possible enhancements to be considered in the future -not dependencies needed for Hoya to work today. With the forthcoming Apache Hadoop 2.1 beta, YARN works very well for hosting HBase clusters. In our test clusters new region servers can go from requested to running in under 20s, much faster than VM-deployment can deliver. The Hoya AM reacts to loss of region servers by immediately re-requesting new instances, making the HBase cluster very robust against server and rack failures. As YARN keeps the Hoya Application Master up and running, the overall system remains highly resilient to failures.

Finally, we can see some ways that HBase could adapt to living in a more agile world. 

Key has to be making HBase region re-allocation location aware -when a cluster starts, for the Region Servers closest to the existing region tables to pick up those regions. This would increase cluster performance on static clusters as well as dynamic ones. Hoya merely encounters the problem more often as it can start up so many more clusters. Once HBase adopts such location awareness, we can enhance Hoya to remember where the region servers were -and ask for workers near those same locations on restart.

Even without these enhancements, Hoya lets us bring up short-lived and long-lived HBase clusters up on YARN, clusters that users can create on the command line or via an API. The cluster description and configuration are saved into HDFS, allowing users to stop a cluster -then recreate it later. A running cluster can have its region server count expanded or contracted -allowing a cluster to scale according to load. WIth the server start time of a few twenty seconds, that offers an opportunity to create dynamically adaptive clusters.

We are building Hoya to demonstrate the capabilities of YARN, to broaden the use cases for HBase by allowing it to be used in mixed workload clusters and to learn how to improve and integrated all the layers of the Hadoop 2.0 stack.  Hoya is a work in progress.  We plan to share our early work on Github under the Apache License, so that we can get feedback and validation.  As Hoya matures, we plan to move it into the Apache Foundation to be closer to Apache's Hadoop and HBase projects. If you are interested in running HBase on YARN:  Come work with us on Hoya!


## Steve Loughran, Devaraj Das and Eric Baldeschwieler