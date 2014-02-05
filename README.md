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

# Hoya


Hoya is a YARN application to deploy existing distributed applications on YARN, 
monitor them and make them larger or smaller as desired -even while 
the cluster is running.


Hoya has a plug-in *provider* architecture to support different applications,
and currently supports Apache HBase and Apache Accumulo.

Clusters can be stopped, "frozen" and restarted, "thawed" later; the distribution
of the deployed application across the YARN cluster is persisted -enabling
a best-effort placement close to the previous locations on a cluster thaw.
Applications which remember the previous placement of data (such as HBase)
can exhibit fast start-up times from this feature.

YARN itself monitors the health of 'YARN containers" hosting parts of 
the deployed application -it notifies the Hoya manager application of container
failure. Hoya then asks YARN for a new container, into which Hoya deploys
a replacement for the failed component. As a result, Hoya can keep the
size of managed applications consistent with the specified configuration, even
in the face of failures of servers in the cluster -as well as parts of the
application itself

## Using Hoya

* [Announcement](src/site/markdown/announcement.md)
* [Installing](src/site/markdown/installing.md)
* [Man Page](src/site/markdown/manpage.md)
* [Examples](src/site/markdown/examples.md)
* [exitcodes](src/site/markdown/exitcodes.md)
* [hoya_cluster_description](src/site/markdown/hoya_cluster_descriptions.md)

## Developing Hoya

* [Architecture](src/site/markdown/architecture.md)
* [Application Needs](src/site/markdown/app_needs.md)
* [Building](src/site/markdown/building.md)
* [Role history](src/site/markdown/rolehistory.md)

# License


  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   (http://www.apache.org/licenses/LICENSE-2.0)
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
