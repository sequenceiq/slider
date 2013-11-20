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
  
# Hoya Release 0.6.6

November 2013

This release is built against the Apache Hadoop 2.2.0 and hbase 0.96.0-hadoop2
artifacts. 


Key changes

## Automatic archive path fixup: --version command dropped

In previous versions of hoya, unless the name of the directory 
in the `hbase.tar` or `accumulo.tar` file matched that of the
version Hoya was set to expect at compile time,
the path to the expanded subdirectory had to be set when creating a cluster,
using the `--version` keyword.

This is no longer the case and the argument has been dropped. Instead the
application master will look inside the expanded archive and determine for itself
what the path is -and fail if it cannot locate `bin/hbase` or `bin/accumulo` under
the single directory permitted in the archive.

This also means that when an HBase or accumulo archive is updated to a later version,
that new version will be picked up automatically.

### Monitor command

The `monitor \<clustername>` command monitors a cluster to verify that

1. it is running at startup
2. it stays running

The monitor operations are a basic "is the YARN application running"
alongside any service specific ones, such as HTTP and RPC port scanning,
other liveness operations. These are the same operations that will be used
in the AM itself to monitor cluster health.

Consult the man page document for usage instructions