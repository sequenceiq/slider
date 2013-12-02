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

# Just some operations for manual runs against steve's secure VM

  export HOYA_JVM_OPTS="-Djava.security.krb5.realm=COTHAM -Djava.security.krb5.kdc=ubuntu -Djava.net.preferIPv4Stack=true"


## Local manual tests



    
    hoya-assembly/target/hoya-assembly-0.5.1-SNAPSHOT-bin/bin/hoya \
      --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 list -D hoya.security.enabled=true
      
      hoya create cluster1 \
      --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
         --role workers 4\
          --zkhosts ubuntu \
          -D hoya.security.enabled=true -S java.security.krb5.realm=COTHAM \
          -S java.security.krb5.kdc=ubuntu \
          --image hdfs://ubuntu:9090/hbase.tar \
          --appconf file:////Users/hoya/Hadoop/configs/master/hbase \
          --roleopt master app.infoport 8080 \
          --roleopt master jvm.heap 128 \
          --roleopt master env.MALLOC_ARENA_MAX 4 \
          --roleopt worker app.infoport 8081 \
          --roleopt worker jvm.heap 128 

 
### bypassing /etc/krb.conf via the -S argument

    bin/hoya create cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true -S java.security.krb5.realm=COTHAM \
    -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
            --role workers 1\
        --zkhosts ubuntu \
        --image hdfs://ubuntu:9090/hbase.tar \
        --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
        --roleopt master app.infoport 8080 \
        --roleopt master jvm.heap 128 \
        --roleopt master env.MALLOC_ARENA_MAX 4 \
        --roleopt worker app.infoport 8081 \
        --roleopt worker jvm.heap 128 
        


    bin/hoya create cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true -S java.security.krb5.realm=COTHAM \
    -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
        --role master 0 \
        --zkhosts ubuntu \
        --image hdfs://ubuntu:9090/hbase.tar \
        --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
        --roleopt master app.infoport 8080 \
        --roleopt master jvm.heap 128 \
        --roleopt master env.MALLOC_ARENA_MAX 4 
        
                
        
    bin/hoya status clu1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true -S java.security.krb5.realm=COTHAM \
    -S java.security.krb5.kdc ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
           
    bin/hoya list \
    --manager ubuntu:8032 \
    -D hoya.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
      -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM
               
               

               
# single master & workre
     
    bin/hoya create cluster3 \
    --zkhosts ubuntu \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
    -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
    -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    --image hdfs://ubuntu:9090/hbase.tar \
    --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
    --roleopt master app.infoport 8080  \
    --role master 1 \
    --role worker 1 
    
    
# one master
     
    bin/hoya create cl1 \
    --zkhosts ubuntu \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
    -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
    -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    --image hdfs://ubuntu:9090/hbase.tar \
    --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
    --role master 1 

# one master env set up
      
     bin/hoya create cl1 \
     --zkhosts ubuntu \
     -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     --image hdfs://ubuntu:9090/hbase.tar \
     --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
     --role master 1  
    
# build but don't deploy single master
     
    bin/hoya build cl1 \
    --zkhosts ubuntu \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
    -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
    -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    --image hdfs://ubuntu:9090/hbase.tar \
    --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
    --roleopt master app.infoport 8080  \
    --role master 1 
         
               
    bin/hoya  status cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
     
               
    bin/hoya  status cl1 -D hoya.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu 
    
    
    bin/hoya  status cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
     
         bin/hoya  status cluster3 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
     
     
               
    bin/hoya  thaw cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true \
     -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
                   
    bin/hoya  freeze cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true \
    -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM   
                      
    bin/hoya  freeze cluster3 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true \
    -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
    
    bin/hoya  destroy cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true \
    -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    
    
      
         
    bin/hoya  emergency-force-kill all \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true -S java.security.krb5.realm=COTHAM \
     -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
     
     
## All configured 
     
     
    bin/hoya create cl1 \
      -S java.security.krb5.realm=COTHAM \
      -S java.security.krb5.kdc=ubuntu \
      --role worker 1\
      --role master 2\
      --zkhosts ubuntu \
      --zkport 2121 \
      --image hdfs://ubuntu:9090/hbase.tar \
      --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
      --roleopt master app.infoport 8080 \
      --roleopt master env.MALLOC_ARENA_MAX 4 \
      --roleopt worker app.infoport 0 \
  
# flex the cluster
  
   bin/hoya flex cl1 \
    --role master 1 \
    --role worker 2 
    
# freeze

    bin/hoya  freeze cl1 
    
# thaw

    bin/hoya  thaw cl1
     
# monitor

    bin/hoya  monitor cl1      

# list all

    bin/hoya  list
     
# list

    bin/hoya  list cl1 
    
# status

    bin/hoya  status cl1 
    
# destroy

    bin/hoya  destroy cl1 