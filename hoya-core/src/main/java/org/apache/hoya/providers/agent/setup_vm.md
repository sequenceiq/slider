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
  
#Steps to setup test VM
* yarn user is used to run the hbase cluster
* /vagrant is assumed to be the file share where packages are available
* Most of these steps will be automated when YARN is used to install agent/spec

*Sample Command*
/var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/HBASE/package/scripts/hbase_regionserver.py START /var/lib/ambari-agent/data/rs_start.json /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/HBASE/package /tmp/strout.txt


##Install resource management library

1. cd /usr/lib/python2.6/site-packages/
2. cp /vagrant/resource_management.tar .
3. tar xvf resource_management.tar
4. chmod -R 755 resource_management/

##Install hbase jar
1. mkdir /share
2. mkdir /sare/hbase
3. cd /share/hbase/
4. cp /vagrant/hbase-0.96.1-hadoop2-bin.tar.tar.tar.gz .
5. gunzip hbase-0.96.1-hadoop2-bin.tar.tar.tar.gz
6. tar xvf hbase-0.96.1-hadoop2-bin.tar.tar.tar
7. chown -R yarn:root hbase-0.96.1-hadoop2/
8. chmod -R 755 hbase-0.96.1-hadoop2/conf/

##Setup agent environment

1. cd /var/lib
2. mkdir ambari-agent
3. mkdir ambari-agent/data
4. mkdir ambari-agent/cache
5. mkdir ambari-agent/cache/stacks
6. mkdir ambari-agent/cache/stacks/HDP
7. chown -R yarn:root ambari-agent
8. cd ambari-agent/cache/stacks/HDP/
9. cp /vagrant/206.tar .
10. tar xvf 206.tar
11. mkdir /var/log/hbase
12. mkdir /var/run/hbase
13. chown yarn:root /var/run/hbase
14. chown yarn:root /var/log/hbase/
15. mkdir /hadoop/hbase
16. chown yarn:root /hadoop/hbase

##Add HDFS root
1. hadoop fs -mkdir /apps
2. hadoop fs -mkdir /apps/hbase
3. hadoop fs -chown yarn:hdfs /apps/hbase