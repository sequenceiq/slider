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

N

This release is built against the Apache Hadoop 2.2.0 and hbase 0.96.0-hadoop2
artifacts. 


Key changes

## Automatic archive path fixup: --version command dropped




## Changing security setup

The `--secure` option has been removed; security is now enabled by configuring
the client with the option `hoya.security.enabled` set to true. This can be
done inside `hoya-client.xml` -turning security on for a target cluster without
any need to declare this on the command line.

## Reworked cluster.json file

There's been some reworking of the `cluster.json` file, with the goal of
"be stable". This isn't something we can guarantee yet, so you have to 
assume that there may still be changes in future.
