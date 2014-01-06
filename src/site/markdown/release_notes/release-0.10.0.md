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
  
# Hoya Release 0.10.0

January 2014

This release is built against the Apache Hadoop 2.2.0 and hbase 0.96.0-hadoop2
artifacts. 


## Key changes

### YARN queue for the application can be specified

The configuration parameter `hoya.yarn.queue` can now be set
in the `hoya-client.xml` file, 

    <property>
      <name>hoya.yarn.queue</name>
      <value>background</value>
    </property>

or on the command line via the `-D` option:

    -D hoya.yarn.queue interactive
  
  
