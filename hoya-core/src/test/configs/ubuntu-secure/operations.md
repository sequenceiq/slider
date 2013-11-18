apache

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
               
               
-D dfs.datanode.kerberos.principal=hdfs/ubuntu@COTHAM \

               
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
    --role worker 1 \
     --version hbase-0.97.0-SNAPSHOT
    
    
     # one master
     
    bin/hoya create cl1 \
    --zkhosts ubuntu \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D hoya.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
    -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
    -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    --image hdfs://ubuntu:9090/hbase.tar \
    --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
    --role master 1  \
     --version hbase-0.97.0-SNAPSHOT

      # one master env set up
      
     bin/hoya create cl1 \
     --zkhosts ubuntu \
     -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     --image hdfs://ubuntu:9090/hbase.tar \
     --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
     --role master 1  \
     --version hbase-0.97.0-SNAPSHOT   
    
    
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
    --role master 1 \
     --hbasever hbase-0.97.0-SNAPSHOT
         
               
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
     
  
  # flex the cluster
  
   hoya flex cl1 \
      --manager master:8032 --filesystem hdfs://master:9090 \
      --role worker 5