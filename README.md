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


Hoya is a YARN an application that can deploy HBase cluster on YARN, 
monitor them and make them larger or smaller as desired -even while 
the cluster is running.
Clusters can be stopped, "frozen" and restarted, "thawed" later.

It works on an Apache Hadoop 2.2 YARN cluster with Hbase 0.96.0 or later.

It also has some initial support for Apache Accumulo, though that
code has not been tested at any scale. 


## Using Hoya

* [Announcement](src/site/markdown/src/site/markdown/announcement.md)
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
