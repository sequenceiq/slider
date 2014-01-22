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
  
# Hoya Release 0.11.1

January 2014

This release is built against the Apache Hadoop 2.2.0 and hbase 0.96.1.1-hadoop2
artifacts. 


## Key changes


### client user name used for HDFS operations in insecure clusters

In a non-Kerberos cluster, the client's username is now propagated
to the Application Master and all containers -and is used to 
identify the cluster for HDFS access. 

As a result, the created databases will correctly belong to the owner
of the cluster, not the identity of the account which yarn applications
run under. 

We've reverted the default cluster security permissions on created directories
to 0750 as appropriate. 

