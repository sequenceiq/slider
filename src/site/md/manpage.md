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

in the directory hoya/target
 
    java -jar target/hoya-0.0.1-SNAPSHOT.jar org.apache.hadoop.hoya.Hoya <ACTION> <OPTIONS>

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