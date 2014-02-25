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

February 2014

This release is built against the Apache Hadoop 2.2.0, HBase-0.98.0RC1
and Accumulo 1.5.0 artifacts. 

Download: [hoya-0.12.1-all.tar.gz]()


## Key changes


### Hoya cluster directory configurable.

The hoya client option `hoya.base.path` can now be used to define the base
directory for Hoya code, rather than it being hard coded to `${user.name}/.hoya"

Thank you to the 42six team for this code -and the tests!

### Hoya AM has a web UI

In the YARN application UI, a URL to a (very minimal) Web status page is provided.

This is very minimal; expect improvements -or contribute them!

### AM Restart code uses introspection.
 
The AM restart code uses introspection to access new fields in the RM protocol
-and is automatically enabled on YARN clusters which support the feature.

The cluster status indicates when the Hoya AM is running with this feature.

### hoya cluster temp dir under `${hoya.base.path}/clusters/${clustername}/tmp`

This was needed to permit some providers to create temporary artifacts
in the AM itself.

### Built against HBase-0.98.0RC1

Be advised that the RC0 and RC1 versions of HBase do not talk to each other well.

Note also there is some ongoing work to support more dynamic role models
for providers, so that providers can add extra roles at run-time, rather than
compile time. This is incomplete, as is some new provider development.