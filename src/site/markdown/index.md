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
  

# Hoya: HBase on YARN




Hoya is an application to dynamically create Apache HBase clusters in an
Apache Hadoop 2.x cluster, using the YARN scheduler to 
create the HBase cluster

Some of the features are:

* Allows users to create on-demand HBase clusters

* Allow different users/applicatins to run different versions of HBase.

* Allow users to configure different Hbase instances differently

* Stop / Suspend / Resume clusters as needed

* Expand / shrink clusters as needed

The Hoya tool is a Java command line application.

It takes in a cluster specification - in terms of the number of regionservers,
the location of HBASE_HOME, the ZooKeeper quorum hosts, the configuration that
the new HBase cluster instance should use and so on.

The tool persists the information as a JSON document into the HDFS.
It also generates the configuration files assuming the passed configuration
directory as a base - in particular, the HDFS and ZooKeeper root directories
for the new HBase instance has to be generated (note that the underlying
HDFS and ZooKeeper are shared by multiple cluster instances). Once the
cluster has been started, the cluster can be made to grow or shrink
using the Hoya commands. The cluster can also be stopped, *frozen*
and later resumed, *thawed*.
      
Hoya implements all its functionality through YARN APIs and the existing
HBase’s shell scripts. The goal of the prototype was to have minimal
code changes and as of this writing, it has required zero code changes in HBase.

We built Hoya to demonstrate the capabilities of YARN, to broaden the use cases
for HBase by allowing it to be used in mixed workload clusters and to learn
how to improve and integrated all the layers of the Hadoop 2.0 stack.  Hoya
is a work in progress.  We plan to share our early work on Github under the
Apache License, so that we can get feedback and validation.  As Hoya
matures, we plan to move it into the Apache Foundation to be closer
to Apache's Hadoop and HBase projects. If you are interested in
running HBase on YARN:  Come work with us on Hoya!

## Using Hoya

* [Announcement](announcement.html)
* [Installing](installing.html)
* [Man Page](manpage.html)
* [Examples](examples.html)
* [Client Configuration] (hoya-client-configuration.html)
* [Client Exit Codes](exitcodes.html)
* [Cluster Descriptions](hoya_cluster_descriptions.html)
* [Security](security.html)

## Developing Hoya

* [Architecture](architecture.html)
* [Application Needs](app_needs.html)
* [Building](building.html)
* [Releasing](releasing.html)
* [Role history](rolehistory.html) (under development)
