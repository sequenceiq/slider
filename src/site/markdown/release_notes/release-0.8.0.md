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
  
# Hoya Release 0.8.0

December 2013

This release is built against the Apache Hadoop 2.2.0 and hbase 0.96.0-hadoop2
artifacts. 


## Key changes


### Command line parsing reworked

How the command line is parsed has changed, with two big implications

1. In previous versions, parameters that were not used by a specific action would
be ignored. Now an unknown parameter (such as `--wait` in the `list` action) results
in an error.

2. The action which you wish to perform MUST be the first item on the command line.
This is similar to how `git` works: the action defines the set of options that
the command line supports

### More and stricter exit codes.

The `EXIT_CLUSTER_IN_USE` (74) is now raised whenever the named cluster
is in use when an action is requested which requires a cluster to not be running.
Examples : thaw, destroy, create
