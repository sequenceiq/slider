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
*  Credentials needed for HBase can be pushed out into the local filesystems of 
the of the worker nodes via some external mechanism (e.g. scp), and protected by
the access permissions of the native filesystem. Any user with access to these
credentials is considered to have been granted such rights.
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

The HBase master is currentll in the same container as the AM. It
must have the keytab and configuration details needed to access the data directory,
and to be trusted by the region servers. It must (as will all the region servers)
have been given the `dfs.namenode.kerberos.principal` configuration value. This is automatically
set in the JSON cluster specificatin by the Hoya client if not explicitly done by the user
as the cluster option `site.dfs.namenode.kerberos.principal` -which is then
inserted into the `hbase-site.xml` file at the same time the Hadoop cluster
filesystem binding is added. This does not need to be done for the 
AM as the AM has the hadoop `core-site.xml` and `yarn-site.xml` configuration files
added to its classpath.

## Securing communications between the Hoya Client and the Hoya AM.

When the AM is deployed in a secure cluster,
it automatically uses Kerberos-authorized RPC channels. The client must acquire a
token to talk the AM. 

This is provided by the YARN Resource Manager when the client application
wishes to talk with the HoyaAM -a token which is only provided after
the caller authenticates itself as the user that has access rights
to the cluster

To allow the client to freeze a Hoya cluster while they are unable to acquire
a token to authenticate with the AM, the `emergency-force-kill <applicationId>` command
requests YARN to trigger cluster shutdown, bypassing the AM. The
`applicationId` can be retrieved from the YARN web UI or the `hoya list` command.
The special application ID `all` will kill all Hoya clusters belonging to that user
-so should only be used for testing or other emergencies.

### How to enable a secure Hoya client

Hoya can be placed into secure mode by setting the property `hoya.security.enabled` to
true. 

This can be done in `hoya-client.xml`:

      <property>
        <name>hoya.security.enabled</name>
        <value>true</value>
      </property>

Or it can be done on the command line

    -D hoya.security.enabled=true

### Adding Kerberos binding properties to the Hoya Client JVM

The Java Kerberos library needs to know the Kerberos controller and
realm to use. This should happen automatically if this is set up as the
default Kerberos binding (on a Unix system this is done in `/etc/krb5.conf`.

If is not set up, a stack trace with kerberos classes at the top and
the message `java.lang.IllegalArgumentException: Can't get Kerberos realm`
will be printed -and the client will then fail.

The realm and controller can be defined in the Java system properties
`java.security.krb5.realm` and `java.security.krb5.kdc`. These can be fixed
in the JVM options, as described in the [Client Configuration] (hoya-client-configuration.html)
documentation.

They can also be set on the Hoya command line itself, using the `-S` parameter.

    -S java.security.krb5.realm=MINICLUSTER  -S java.security.krb5.kdc=hadoop-kdc

### Java Cryptography Exceptions 


When trying to talk to a secure, cluster you may see the message:

    No valid credentials provided (Mechanism level: Illegal key size)]


## Putting it all together: examples


### Example 1: creating a secure cluster


This example creates a secure 4-HBase-worker-node cluster, specifying
the JVM kerberos bindings as part of the arguments. 

This binds to a a cluster where the YARN RM, HDFS Namenode,
Zookeeper and the Kerberos daemon are running on a server called `master`,
with the Kerberos domain `MINICLUSTER`

The user must have an up to date TGT token from the Kerberos service, as granted
via a `kinit` call.

      hoya create cluster1 \
      --manager master:8032 --filesystem hdfs://master:9090 \
         --role workers 4\
          --zkhosts master \
          -D hoya.security.enabled=true -S java.security.krb5.realm=MINICLUSTER \
          -S java.security.krb5.kdc=master \
          --image hdfs://master:9090/hbase.tar \
          --appconf file:////Users/hoya/Hadoop/configs/master/hbase \
          --roleopt master app.infoport 8080 \
          --roleopt master jvm.heap 512 \
          --roleopt master env.MALLOC_ARENA_MAX 4 \
          --roleopt worker jvm.heap 512 

The HBase configuration file must contain the definitions of the 
principals of the cluster
  
    <property>
      <name>hbase.master.kerberos.principal</name>
      <value>hoya/master@MINICLUSTER</value>
    </property>
    
    <property>
      <name>hbase.master.keytab.file</name>
      <value>/home/hoya/conf/hoya.keytab</value>
    </property>
  
    <property>
      <name>hbase.regionserver.kerberos.principal</name>
      <value>hoya/master@MINICLUSTER</value>
    </property>
    
    <property>
      <name>hbase.regionserver.keytab.file</name>
      <value>/home/hoya/conf/hoya.keytab</value>
    </property>


### Example: listing the status of the cluster

Here the krb5 configuration file is expected to be set up
to define the realm and kerberos server to use, the JVM system
properties can be omitted from the command line.
        
    bin/hoya status cluster1 \
    --manager master:8032 --filesystem hdfs://master:9090 \
     -D hoya.security.enabled=true \
     -D yarn.resourcemanager.principal=yarn/master@MINICLUSTER \
     -D dfs.namenode.kerberos.principal=hdfs/master@MINICLUSTER 

This command uses the Hoya Client to AM authentication process -if
for any reason the client cannot authenticate with the Hoya AM, it
will fail.



### Example: freezing the cluster

Again the krb5 configuration file is expected to be set up
to define the realm and kerberos server to use.

        
    bin/hoya freeze cluster1 \
    --manager master:8032 --filesystem hdfs://master:9090 \
     -D hoya.security.enabled=true \
     -D yarn.resourcemanager.principal=yarn/master@MINICLUSTER \
     -D dfs.namenode.kerberos.principal=hdfs/master@MINICLUSTER 

This command also talks to the HoyaAM, so will fail if authentication
does not succeed.

### Example: listing the active clusters

Listing the clusters is a direct conversation with the YARN RM

    bin/hoya list \
    --manager master:8032 \
     -D hoya.security.enabled=true \
     -D yarn.resourcemanager.principal=yarn/master@MINICLUSTER \
     -D dfs.namenode.kerberos.principal=hdfs/master@MINICLUSTER

Although it doesn't talk to the filesystem, the current security checking
code in the client still tries to verify that this principal is set
-a check done to ensure that operations fail early with a meaningful message,
rather than later with a more obscure one. 

### Example: setting  hoya-client.xml up


The file 'conf/hoya-client.xml' can be set up with the details of the filesystem,
YARN RM and the relevant security options, allowing them to be dropped from the
command line

    <property>
      <name>yarn.resourcemanager.address</name>
      <value>master:8032</value>
    </property>
    
    <property>
      <name>fs.defaultFS</name>
      <value>hdfs://master:9090</value>
    </property>
    
    <property>
      <name>hoya.security.enabled</name>
      <value>true</value>
    </property>
    
    <property>
      <name>yarn.resourcemanager.principal</name>
      <value>yarn/master@MINICLUSTER</value>
    </property>
    
    <property>
      <name>dfs.namenode.kerberos.principal</name>
      <value>hdfs/master@MINICLUSTER</value>
    </property>
    

### Example : listing the clusters with hoya-client.xml set up


With the `hoya-client.xml' file set up, configuration is much simpler:

    bin/hoya  status cluster1 -D hoya.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=master 

### Example: setting up the JVM options


    export HOYA_JVM_OPTS="-Djava.security.krb5.realm=MINICLUSTER -Djava.security.krb5.kdc=master -Djava.net.preferIPv4Stack=true"


### Example: listing the cluster with hoya-client.xml and the JVM options set up

    bin/hoya  status cluster1 -D hoya.security.enabled=true

### Example: destroying the cluster with hoya-client.xml and the JVM options set up

    bin/hoya  destroy cluster1 -D hoya.security.enabled=true

## Useful Links

1. [Adding Security to Apache Hadoop](http://hortonworks.com/wp-content/uploads/2011/10/security-design_withCover-1.pdf)
1. [The Role of Delegation Tokens in Apache Hadoop Security](http://hortonworks.com/blog/the-role-of-delegation-tokens-in-apache-hadoop-security/)
1. [Chapter 8. Secure Apache HBase](http://hbase.apache.org/book/security.html)
1. Hadoop Operations p135+
1. [Java Kerberos Requirements](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/KerberosReq.htmla)
1. [Troubleshooting Kerberos on Java](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html)
1. For OS/X users, the GUI ticket viewer is `/System/Library/CoreServices/Ticket\ Viewer.app`


