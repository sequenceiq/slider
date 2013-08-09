<!---
~~ Licensed under the Apache License, Version 2.0 (the "License");
~~ you may not use this file except in compliance with the License.
~~ You may obtain a copy of the License at
~~
~~   http://www.apache.org/licenses/LICENSE-2.0
~~
~~ Unless required by applicable law or agreed to in writing, software
~~ distributed under the License is distributed on an "AS IS" BASIS,
~~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
~~ See the License for the specific language governing permissions and
~~ limitations under the License. See accompanying LICENSE file.
-->

# hoya: HBase on YARN

## NAME

hoya - HBase on YARN

## SYNOPSIS

The hoya program is the main interface for managing HBase clusters on a YARN-managed datacenter.
The program can be used to create, pause, and shutdown HBase clusters. It can also be used to list current clusters.
 
## CONCEPTS

1. A *Hoya cluster* represents a short-lived or long-lived set of HBase servers; one HBase Master and one or more HBase Region Servers

1. A *cluster* is built by deploying an *image* across multiple nodes in a YARN cluster.

1. An *image* is a *tar.gz* file containing a supported version of HBase.

1. Images are kept in the HDFS filesystem and identified by their path names; filesystem permissions can be used to share images amongst users.

1. An *image configuration* is a directory that is overlaid file-by-file onto the conf/ directory inside the HBase image.

1. Users can have multiple image configurations -they too are kept in HDFS, identified by their path names, and can be shared by setting the appropriate permissions, along with a configuration template file.

1. Only those files provided in the image configuration directory overwrite the default values contained in the image; all other configuration files are retained.

1. Late-binding properties can also be provided to a cluster at create time.

1. Hoya will overwrite some of the HBase configuration properties to configure the dynamically created HBase cluster nodes to bind correctly to each other.

1. A *cluster state directory* is a directory created in HDFS describing the cluster; it records user-specified properties including the image and image configuration paths, overridden properties, and creation-time node requirements.

1. The cluster state directory also contains dynamically created information as to the location of HBase region servers -this is used to place the region servers close to their previous locations -ideally on the server used before, falling back to the same rack and then elsewhere in the same cluster.

1. A user can create a cluster using a named image.

1. A cluster can be stopped, saving its final state to its cluster state directory. All the HBase processes are shut down.

1. A stopped cluster can be started -a new set of HBase processes are started on or near the servers where the earlier processes were previously running.

1. A stopped cluster can be destroyed. simply by deleting the cluster state directory.

1. A stopped cluster can be reimaged. This can update the cluster's HBase version and its configuration. When the cluster is started, the changes are picked up.

1. Running clusters can be listed. 

1. All images and clusters are private to the user account that created them.

1. Users can flex a cluster: adding or removing nodes dynamically.
If the cluster is running, the changes will have immediate effect. If the cluster
is stopped, the flexed cluster size will be picked up when the cluster is next
started.

## Invoking Hoya

build hoya

    mvn clean install -DskipTests

in the same directory:
 
    java -jar target/hoya-0.0.1-SNAPSHOT.jar org.apache.hadoop.hoya.Hoya <ACTION> <OPTIONS>

* Later versions of Hoya will use a different version number

* this command line entry point relies on target/lib containing all the dependency libraries.
To use Hoya elsewhere, all JARs in that directory must be on the classpath.

* Currently the log4j.properties file is embedded inside hoya.jar

## COMMAND-LINE OPTIONS

### --conf configuration.xml

Configure the Hoya client. This allows the filesystem, zookeeper instance and other properties to be picked up from the configuration file, rather than on the command line.

Important: *this configuration file is not propagated to the HBase cluster configuration*. It is purely for configuring the client itself. 

### --hbasehome localpath

A path to hbase home for a hoya cluster. If set when a Hoya cluster is created, the cluster will run with the version of HBase pre-installed on the nodes.

### --image dfspath

The full path in Hadoop HDFS  to a .tar or .tar.gz file containing an HBase image.


### --confdir dfspath

The full path in Hadoop HDFS (or other shared FS)  to the configuration directory containing the template cluster specification. The path must be on a filesystem visible to all nodes in the YARN cluster.

Only one configuration directory can be specified.

### --generated_confdir localpath

A path to a local directory where a copy of the generated configuration directory should be written. This is to provide a copy of the configuration during cluster creation.
  

### -m, --manager url

URL of the YARN resource manager

### --zkport port 

The port on which the zookeeper processes are listening.

### --zkhosts host1[,host2,host3] 

The list of hosts on which the ZK quorum is running.

### -D name=value

Define an HBase configuration option which overrides any options in the configuration XML files of the image or in the image configuration directory. The values will be persisted. Configuration options are only passed to the cluster when creating or reconfiguring a cluster.

### --fs filesystem-uri

Use the specific filesystem URI as an argument to the operation.

### --format {xml,properties,text}
The format for returned documents. xml is the Hadoop XML configuration format; properties Java  properties.

### --managers count

The number of HBase managers. Only 0 and 1 are currently supported. Default: 1

### --workers count

the number of workers desired when creating or flexing a cluster.


### --wait timeout

When creating or starting a cluster, wait the for the Hoya Cluster itself to start running. It can still take time after this for HBase to be live.

When stopping a cluster -the time for the YARN application to be finished.

### --amqueue queue

Application Manager Queue Name. Applicable (and optional) in cluster create and start operations. Default value: "default"

### --ampriority priority

Application Manager Priority. Applicable (and optional) in cluster create and start operations. Default value: 0


## Testing and debugging arguments

These options are provided for internal testing of Hoya, and are documented for completeness. No guarantees as to the stability of the commands can be made, and they must be considered unsupported.

### --Xtest

This notifies the application that this is a test run, and that the application should behave in a way to aid testing. Currently all this does is

1. Limit the number of attempts to start the AM to one.
1. Enable verbose output in the client-AM RPC

### --Xhbase-master-command
The single command to execute on the HBase master as opposed to "master", in the command sequence:

    hbase master start

for example, if the parameter was
  
    --Xhbase-master-command version

Hoya would would run the HBase master with the command

    hbase version

This would not actually create the master -as stated, it is for testing purposes.

## Actions

CLUSTER COMMANDS

### list \<cluster>
List HBase running clusters visible to the user.

If a cluster name is given and there is no running cluster with that name, an error is returned. 

### status \<cluster>

Get the status of the HBase cluster at of the given name.

The --user option can be used to specify a different user's clusters to list, to list all hoya instances use
--user ""

### create \<cluster> --fs filesystem --workers workers --confdir dir --zkhosts zkhosts \[--image path] \[--hbasehome hbasehomedir] \[--zkport port] \[--hbasezkpath zkpath]\[ \[--waittime time] \[--workerheap heapsize] \[--masters masters] \[--masterheap heapsize]
Create a cluster of the given ZK name, using the specified image. The minimum and maximum nodes define the number of region servers to be created; an HBase master is always created. If a configuration directory is specified, it's configuration files override those in the image. 

The waittime parameter, if provided, specifies the time to wait until the YARN application is actually running. Even after the YARN application has started, there may be some delay for the HBase cluster to start up.

### destroy \<cluster>

Destroy a (stopped) cluster.

Important: This deletes all the database data.

### stop \<cluster>  \[--waittime time]
Stop the cluster. The HBase cluster is scheduled to be destroyed. The cluster settings are retained in HDFS.

The --waittime argument can specify a time in seconds to wait for the cluster to be destroyed.

If an unknown (or already stopped) cluster is named, no error is returned.

### start \<cluster>

Resume the cluster: recreate the cluster from its previous state. This will include a best-effort attempt to create the same number of nodes as before, though their locations may be different.
The same zookeeper bindings as before will be used.

### islive \<cluster>

probe for a cluster being live. If not, an error code is returned.
The probe does not check the status of the HBase services, merely that a cluster has been deployed


### flex \<cluster> \[--workers count] \[--persist true|false]

Flex the number of workers in a cluster to the new value. If greater than before -nodes will be added. If less, nodes will be removed  from the cluster. 
The persist flag (default = true) indicates whether or not the new worker count should be persisted and used the next time the cluster is started up. Set this to false if the flexed cluster size is only to be applied to a live cluster.

This operation has a return value of 0 if the size of a running cluster was changed. 

It returns -1 if there is no running cluster, or the size of the flexed cluster matches that of the original -in which case the cluster state does not change.

### getconf \<cluster>  \[--out file] \[--format xml|properties]

Get the configuration properties needed for hbase clients to connect to the cluster. Hadoop XML format files (the default) and Java properties files can be generated.
The output can be streamed to the console in "stdout", or it can be saved to a file.

## Unimplemented

### UNIMPLEMENTED: getsize \<cluster> \[-count count]
 
Return the number of region servers in the cluster as the return value of the operation.
If a count parameter is provided, then the return code is success, 0, if and only if the number of region servers equals the count. The HBase Master is not included in the size count.


### UNIMPLEMENTED: reconfigure \<cluster> --confdir path
Update a cluster with a new configuration. This operation is only valid on a stopped cluster. The new configuration will completely replace the existing configuration.

### UNIMPLEMENTED: reimage \<cluster> --image path
Update a cluster with a new configuration. This operation is only valid on a stopped cluster. The new configuration will completely replace the existing configuration.

