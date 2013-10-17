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

This document discusses the design, implementation and use of Hoya
to deploy secure applications on a secure Hadoop cluster.

### Important:
 
This document does not cover Kerberos, how to secure a Hadoop cluster, Kerberos
command line tools or how Hadoop uses delegation tokens to delegate permissions
round a cluster. These are assumed, though some links to useful pages are
listed at the bottom. 


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
1. As the data directories for a hoya cluster are created under the home
directories of that user, the principals representing all role instances
in the clusters *MUST* have read/write access to these files. This can be
done with a shortname that matches that of the user, or by requesting
that Hoya create a directory with group write permissions -and using LDAP
to indentify the application principals as members of the same group
as the user.


## Requirements


### Needs
*  Hoya and HBase to work against secure HDFS
*  Hoya to work with secure YARN.
*  Hoya to start a secure HBase cluster
*  Kerberos and ActiveDirectory to perform the authentication.
*  Hoya to only allow cluster operations by authenticated users -command line and direct RPC. 
*  Any Hoya Web UI and REST API for Ambari to only allow access to authenticated users.
*  The Hoya database in ~/.hoya/clusters/$name/data to be writable by HBase


### Short-lived Clusters
*  Cluster to remain secure for the duration of the Kerberos tokens issued to Hoya.


### Long-lived Clusters

*  Hoya cluster and HBase instance to remain functional and secure over an indefinite period of time.

### Initial Non-requirements
*  secure audit trail of cluster operations.
*  multiple authorized users being granted rights to a Hoya Cluster (YARN admins can always kill the Hoya cluster.
*  More than one HBase cluster in the YARN cluster belonging to a single user (irrespective of how they are started).
*  Any way to revoke certificates/rights of running containers.

### Assumptions
*  Kerberos is running and that HDFS and YARN are running Kerberized.
*  LDAP cannot be assumed. 
*  Credentials needed for HBase can be pushed out into the local filesystems of the of the worker nodes via some external mechanism (e.g. scp), and protected by the access permissions of the native filesystem. Any user with access to these credentials is considered to have been granted such rights.
*  These credentials can  outlive the duration of the HBase containers
*  The user running HBase has the same identity as that of the HBase cluster.

## Design


1. The Hoya user is expected to have their own Kerberos principal, and have used `kinit`
 or equivalent to authenticate with Kerberos and gain a (time-bounded) TGT
1. The Hoya user is expected to have their own principals for every host in the cluster of the form
  username/hostname@REALM
1. A keytab must be generated which contains all these principals -and distributed
to all the nodes in the cluster with read access permissions to the user.
1. When the user creates a secure cluster, they provide the standard HBase kerberos options
to identify the principals to use and the keytab location.

The Hoya Client will talk to HDFS and YARN authenticating itself with the TGT,
talking to the YARN and HDFS principals which it has been configured to expect.

This can be done as described in [Hoya Client Configuration] (hoya-client-configuration.html) on the command line as

     -D yarn.resourcemanager.principal=yarn/master@LOCAL 
     -D dfs.namenode.kerberos.principal=hdfs/master@LOCAL

The Hoya Client will create the cluster data directory in HDFS with `rwx` permissions for  
user `r-x` for the group and `---` for others. (these can be configurable as part of the cluster options), 

It will then deploy the AM, which will (somehow? for how long?) retain the access
rights of the user that created the cluster.

The Hoya AM will read in the JSON cluster specification file, and instantiate the
relevant number of role instances. 

The HBase master will be executed in the same container as the AM. It
must have the keytab and configuration details needed to access the data directory,
and to be trusted by the region servers. It must (as will all the region servers)
have been given the `dfs.namenode.kerberos.principal` configuration value. This is automatically
set in the JSON cluster specificatin by the Hoya client if not explicitly done by the user
as the cluster option `site.dfs.namenode.kerberos.principal` -which is then
inserted into the `hbase-site.xml` file. [This does not need to be done for the 
AM as the AM has the hadoop core-site.xml and yarn-site.xml configuration files
added to its classpath.]

## Securing communications between the Hoya Client and the Hoya AM.

This is still a work in progress. When the AM is deployed in a secure cluster,
it automatically uses Kerberos-authorized RPC channels. The client must acquire a
token to talk the AM. 

To allow the client to freeze a Hoya cluster while they are unable to acquire
a token to authenticate with the AM, the `emergency-force-kill $applicationId` command
will request YARN to trigger cluster shutdown, bypassing the AM. The
`applicationId` can be retrieved from the YARN web UI or the `hoya list` command.
The special application ID `all` will kill all YARN clusters, so should only be used
for testing.

## Useful Links

1. [Adding Security to Apache Hadoop](http://hortonworks.com/wp-content/uploads/2011/10/security-design_withCover-1.pdf)
1. [The Role of Delegation Tokens in Apache Hadoop Security](http://hortonworks.com/blog/the-role-of-delegation-tokens-in-apache-hadoop-security/)
1. [Chapter 8. Secure Apache HBase](http://hbase.apache.org/book/security.html)
1. Hadoop Operations p135+
1. [Java Kerberos Requirements](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/KerberosReq.htmla)
1. [Troubleshooting Kerberos on Java](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html)
1. OS/X users, the GUI ticket viewer is `/System/Library/CoreServices/Ticket\ Viewer.app`


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

 
### bypassing /etc/krb.conf via the -S argument

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

               
     # single master & workre
     
    bin/hoya create cl1 \
    --zkhosts ubuntu \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    --secure -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
    -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
    -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    --image hdfs://ubuntu:9090/hbase.tar \
    --appconf file:///Users/stevel/Projects/Hortonworks/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
    --roleopt master app.infoport 8080  \
    --role master 1 \
    --role worker 1 \
     --hbasever hbase-0.97.0-SNAPSHOT
    
    
     # no master
     
    bin/hoya create cl1 \
    --zkhosts ubuntu \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    --secure -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
    -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
    -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    --image hdfs://ubuntu:9090/hbase.tar \
    --appconf file:///Users/stevel/Projects/Hortonworks/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
    --role master 0  \
     --hbasever hbase-0.97.0-SNAPSHOT
    
     # build but don't deploy single master
     
    bin/hoya build cl1 \
    --zkhosts ubuntu \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    --secure -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
    -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
    -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    --image hdfs://ubuntu:9090/hbase.tar \
    --appconf file:///Users/stevel/Projects/Hortonworks/Projects/hoya/hoya-core/src/test/configs/ubuntu-secure/hbase \
    --roleopt master app.infoport 8080  \
    --role master 1 \
     --hbasever hbase-0.97.0-SNAPSHOT
         
               
    bin/hoya  status cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    --secure -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
     -D hoya.kerberos.principal=stevel/ubuntu@COTHAM
               
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
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    
    
      
         
    bin/hoya  emergency-force-kill all \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    --secure -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
     
    