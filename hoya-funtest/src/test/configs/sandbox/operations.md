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


    export HOYA_CONF_DIR=/Users/stevel/Projects/Hortonworks/Projects/hoya/hoya-core/src/test/configs/sandbox/hoya

## Local manual tests



    
    hoya-assembly/target/hoya-assembly-0.5.1-SNAPSHOT-bin/bin/hoya \
      --manager sandbox:8032 --filesystem hdfs://sandbox.hortonworks.com:8020 list -D hoya.security.enabled=true
      
      hoya create cluster1 \
         --role worker 4\
          --zkhosts sandbox \
          --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
          --appconf file:////Users/hoya/Hadoop/configs/master/hbase \
          --roleopt master app.infoport 8190 \
          --roleopt master jvm.heap 128 \
          --roleopt worker app.infoport 8191 \
          --roleopt worker jvm.heap 128 

 
### bypassing /etc/krb.conf via the -S argument

    bin/hoya create cl1 \
    --manager sandbox:8032 --filesystem hdfs://sandbox.hortonworks.com:8020 \
            --role worker 1\
            --role master 0\
        --zkhosts sandbox  \
        --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
        --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/sandbox/hbase \
        --roleopt master app.infoport 8180 \
        --roleopt master jvm.heap 128 \
        --roleopt master env.MALLOC_ARENA_MAX 4 \
        --roleopt worker app.infoport 8181 \
        --roleopt worker jvm.heap 128 
        


    bin/hoya create cl1 \
        --role master 0 \
        --zkhosts sandbox  \
        --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
        --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/sandbox/hbase \
        --roleopt master app.infoport 8180 \
        --roleopt master jvm.heap 128 \
        --roleopt master env.MALLOC_ARENA_MAX 4 
        
                
        
    bin/hoya status clu1 \
    --manager sandbox:8032 --filesystem hdfs://sandbox.hortonworks.com:8020 \
           
    bin/hoya list \
    --manager sandbox:8032 \
               

               
# single master & workre
     
    bin/hoya create cluster3 \
    --zkhosts sandbox  \
    --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
    --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/sandbox/hbase \
    --roleopt master app.infoport 8180  \
    --role master 1 \
    --role worker 1 
    
    
# one master
     
    bin/hoya create cl1 \
    --zkhosts sandbox   \
    --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
    --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/sandbox/hbase \
    --role master 1 

# one master env set up
      
     bin/hoya create cl1 \
     --zkhosts sandbox   \
     --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
     --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/sandbox/hbase \
     --role master 1  \
     --role worker 1  
    
# build but don't deploy single master
     
    bin/hoya build cl1 \
    --zkhosts sandbox \
     \
    --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
    --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/sandbox/hbase \
    --roleopt master app.infoport 8180  \
    --role master 1 
         

               
    bin/hoya  status cl1 
    
    
    bin/hoya  status cl1 
     
   
     
     
               
    bin/hoya  thaw cl1  
                   
    bin/hoya  freeze cl1  
    bin/hoya  freeze cluster3  
    bin/hoya  destroy cl1  
    
    
      
         
    bin/hoya  emergency-force-kill all 
     
     
## All configured 
     
     
    bin/hoya create cl1 \
      --role worker 1\
      --role master 2\
      --zkhosts sandbox \
      --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz  \
      --appconf file:///Users/stevel/Projects/hoya/hoya-core/src/test/configs/sandbox/hbase \
      --roleopt master app.infoport 8180 \
      --roleopt master env.MALLOC_ARENA_MAX 4 \
      --roleopt worker app.infoport 0 \
  
### flex the cluster
  
   bin/hoya flex cl1 \
    --role master 1 \
    --role worker 2 
    
### freeze

    bin/hoya  freeze cl1 
    
    bin/hoya  freeze cl1 --force 
    
### thaw

    bin/hoya  thaw cl1 -D hoya.yarn.queue.priority=5 -D hoya.yarn.queue=default
    
    
### thaw with bad queue: _MUST_ fail
    
    bin/hoya  thaw cl1 -D hoya.yarn.queue=unknown
     
### monitor

    bin/hoya  monitor cl1      

### list all

    bin/hoya  list
     
### list

    bin/hoya  list cl1 
    
### status

    bin/hoya  status cl1 
    
### destroy

    bin/hoya  destroy cl1 