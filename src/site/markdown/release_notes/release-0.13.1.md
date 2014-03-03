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
  
# Hoya Release 0.12.1

March 2014

This release is built against the Apache Hadoop 2.3.0, HBase-0.98.0RC1
and Accumulo 1.5.0 artifacts. 

Download: [hoya-0.13.1-all.tar.gz]()


## Key changes


### Improved Hoya AM web UI

The Web UI has more features

### Built against Hadoop 2.3.0

This release has been built using the Hadoop 2.3.0 libraries.

*This should not affect the ability to deploy and run on Hadoop 2.2.0 clusters*

As part of our release process we run functional tests of Hoya on a HDP-2.0
cluster running Hadoop 2.2.0, as well as a secure Hadoop 2.3.0 cluster.


## Other points

* There is some ongoing work to support more dynamic role models for providers,
  so that providers can add extra roles at run-time, rather than compile time.
  This is incomplete, as is some new provider development.
* Part of the release process involved rewinding the master branch two revisions;
  this was the only way to merge in the develop branch reliably. If a git update
  complains, this is the root cause -sorry.
