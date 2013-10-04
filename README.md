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
monitor them and make them larger or smaller as desired. Clusters
can be stopped and restarted later.

It is designed to work on Apache Hadoop 2.1, which, until released
with a compatible HBase version, needs some manual intervention
to build locally.

It also has some initial support for Apache Accumulo, though that
code has not been tested at any scale. 

* [Project Home](https://github.com/hortonworks/hoya)
* [Announcement](hoya-core/src/site/md/announcement.md)
* [Man Page](hoya-core/src/site/md/manpage.md)
* [Building Hoya](hoya-core/src/site/md/building.md)
* [Hoya Application Needs](hoya-core/src/site/md/app_needs.md)

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
