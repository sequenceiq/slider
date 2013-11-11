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
  
# Hoya Release 0.6.2: multiple master and role history support.

November 11, 2013

This release is built against the Apache Hadoop 2.2.0 and hbase 0.96.0-hadoop2
artifacts. 

It has two major new features.

## Multiple Masters

The HBase and Accumulo Masters are no longer forked off the Hoya Application
Master -they are now deployed in their own containers. The Hoya AM will monitor
these processes and restart them if they fail. 

This now permits clusters to be defined with more than one master; Hoya will
request a YARN container for each instance and deploy them all. 

_Important_:  if you fix the web UI port of the application master via the
`app.infoport` property, only one of the master instances will start if
YARN allocates both containers on the same node. Please do not attempt to
fix a web UI port when running with multiple masters.

## Role History 

This is the most significant change of the 0.6.2 release: a Hoya cluster
remembers on which hosts instances of specific roles
were previous deployed -and asks YARN for them again when recreating or
expanding a cluster.

This allows the HBase Master to reassign the same tables to the Region Servers
on a specific server as were used before, and so start up faster.

For full details, please consult the [Role History Design Document]
(https://github.com/hortonworks/hoya/blob/develop/src/site/markdown/rolehistory.md).

As this is new, even though we have extensive unit tests, we still expect
to see some surprises in larger clusters. Please do not hesitate to report
issues.

For the curious, the history files are stored as Avro JSON files
in the `history/` subdirectory of each cluster. If bug reports are to be
filed against role history/placement issues, adding the most recent of these
would be convenient (The filenames are generated to ensure that they sort in order).



