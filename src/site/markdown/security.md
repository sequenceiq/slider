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
  
# Hoya Security

## Concepts

Hoya runs in secure clusters, but with restrictions 

1. The keytabs to allow a worker to authenticate with the master must
be distributed in advance: Hoya does not attempt to pass these around.
1. Until the location of Hoya node instances can be strictly limited to
a set of nodes (a future YARN feature), the keytabs must be passed to
all the nodes in the cluster in advance, *and made available to the
user creating the cluster*
1. due to the way that HBase and accumulo authenticate worker nodes to
the masters, any HBase node running on a server must authenticate as
the same principal, and so have equal access rights to the HBase cluster.


## Local manual tests



    
    hoya-assembly/target/hoya-assembly-0.5.1-SNAPSHOT-bin/bin/hoya \
      --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 list --secure
      
      hoya-assembly/target/hoya-assembly-0.5.1-SNAPSHOT-bin/bin/hoya \
      --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
          create cl1 \
         --role workers 4\
          --zkhosts ubuntu \
          --image hdfs://ubuntu:9090/hbase.tar \
          --appconf file:////Users/hoya/Hadoop/configs/master/hbase \
          --roleopt master app.infoport 8080 \
          --roleopt master jvm.heap 128 \
          --roleopt master env.MALLOC_ARENA_MAX 4 \
          --roleopt worker app.infoport 8081 \
          --roleopt worker jvm.heap 128 

 
## bypassing /etc/krb.conf via the -S argument

    bin/hoya create cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    --secure -S java.security.krb5.realm=COTHAM \
    -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
            --role workers 1\
        --zkhosts ubuntu \
        --image hdfs://ubuntu:9090/hbase.tar \
        --appconf file:///Users/stevel/Projects/Hortonworks/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
        --roleopt master app.infoport 8080 \
        --roleopt master jvm.heap 128 \
        --roleopt master env.MALLOC_ARENA_MAX 4 \
        --roleopt worker app.infoport 8081 \
        --roleopt worker jvm.heap 128 
        
        
    bin/hoya status clu1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    --secure -S java.security.krb5.realm=COTHAM \
    -S java.security.krb5.kdc ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.datanode.kerberos.principal=hdfs/ubuntu@COTHAM 
           
    bin/hoya list \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    --secure -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.datanode.kerberos.principal=hdfs/ubuntu@COTHAM \
               
               
-D dfs.datanode.kerberos.principal=hdfs/ubuntu@COTHAM \

               
    bin/hoya create cl1 \
    --zkhosts ubuntu \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    --secure -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
    -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    --image hdfs://ubuntu:9090/hbase.tar \
    --appconf file:///Users/stevel/Projects/Hortonworks/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
    --roleopt master app.infoport 8080  \
    --role master 1
    
     
               
    bin/hoya  thaw cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    --secure -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
                   
    bin/hoya  freeze cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    --secure -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
    
    bin/hoya  destroy cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    --secure -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
    
      
         
    bin/hoya  emergency-force-kill application_1381252124398_0003 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    --secure -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
    