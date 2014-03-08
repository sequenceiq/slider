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

#Apps on YARN CLI

This document describes the CLI to deploying YARN applications. It is not the formal definition of the behaviour, which is described elsewhere [It will be defined as per the [hoya-cli](https://github.com/hortonworks/hoya/blob/master/src/site/markdown/specification/cli-actions.md)].

## Goals of the CLI

1. Deploy and Manage long-lived applications deployed on a YARN cluster.

2. Work with a Hoya-deployed application, as well as those deployed/deployable via a management system (e.g. Ambari or similar).

### Other Requirements

1. Be usable on the command line of Unix and Windows systems.

2. Return error codes sufficient for functional tests.

3. work outside the YARN cluster/not need a local installation of the hadoop stack

4. if there is a local hadoop stack, pick up its site.xml and classpath

5. Work with secure clusters.

6. Work with the REST APIs implemented by the services (ultimately)

## Operations

### build

build an application specification, but do not start it. For hoya this involves declaring the provider, options for the roles, etc. The provider is invoked during the build process, and can set default values for options & roles.

For a YARN application it would involve declaring the application. The default values of options and roles would be built from the application metadata. (Note that for Hoya, using this CLI would trigger an automatic use of the agent profile.

### create

Build + start. Maybe just have this create the cluster details and drop the build operation.

### start/thaw <cluster> 

Start an existing cluster

### stop/freeze<cluster>  [--force]

Stop the cluster. 

The --force operation tells YARN to kill the cluster without involving the AM. This has proven useful in testing and handling expired security credentials in the AM.

### flex <cluster> [parameters]?

 -update cluster size; configurations may not propagate

### destroy <cluster> 

destroy a (stopped) cluster. The stop check is there to prevent accidentally destroying a cluster in use

### status <cluster> 

### exists <cluster> [--accepted] [--started] [--live] [--finished] [--failed] [--stopped] 

Query for the existence of a cluster. If any state conditions are listed, the probe succeeds if and only iff the cluster is in the given state

### listclusters [--accepted] [--started] [--live] [--finished] [--failed] [--stopped] 

List all clusters, optionally in the named specific states. 

### list <cluster> 

List a cluster. If there is a record of a cluster in a failed/finished state AND there is no cluster of the same name live, the finished cluster is listed. Otherwise, the running cluster is listed.

If there a no instances of a cluster in the YARN history, the cluster is looked for in the cluster directory, and listed if present.

### getfile <cluster> [--file <filename>  [--dir destdir|--outfile destfile]>]

list/retrieve any files published by the cluster.

if no --file option is provided available files are listed

If a --file is specified, it is downloaded to the current directory with the specified filename, unless a destination directory/filename is provided

### getproperties <cluster> [--format xml|json|properties|csv] [<--outfile file>]

get the properties of a cluster (content in /clientProperties)

outfile:= output file name rather than stdout

format values:

XML => Hadoop XML format

JSON => JSON key:value map

properties= Java properties file (with escaping)

csv = CSV format "key","value"

### getproperty <cluster> <propertyname>

Retrieves a single property of a cluster and emits it to stdout.

If the property is unknown, an error code is emitted.

### history <cluster> 

### kill --containers [containers] --roles [roles] --nodes [nodes]

Kill listed containers, everything in specific roles, or on specific nodes. This can be used to trigger restart of services and decommission of nodes

### reconfigure

rebuild configuration of nodes, stop all of them and reschedule a new set

### setoption <cluster> <option=value>* 

Patch cluster.json with a specific option

### setroleoptions <cluster> <role> <option=value>*

Patch the named roles

### wait **<cluster> [started|live|stopped] --timeout <time> **

Block waiting for a cluster to enter the state. Can fail if the cluster stops while waiting for it to be started/live.

## Specifying configuration parameters when creating a cluster

The Hoya cli requires all configuration parameters to be passed on the command line, this builds up the JSON configuration.

This has worked for simple configurations, but scales badly as the command soon spans multiple lines, a single error breaks it and it is hard to use. You end up creating the command in a text editor and repeatedly trying it until it works.

Proposal: support a --template <file> that takes a template cluster.json and copies over all the roles and options, *then allows command line parameters to override this*. This would let one clone existing cluster files as well as create new ones.

*For this to work, **all instance-specific hoya-internal properties must be moved out of options,** as proposed in the updated cluster specification document.*

## How the CLI is itself configured

Currently Hoya is configured by

1. JVM properties set with -S name=value . Useful for setting Kerberos credentials and other JVM options on the command line. As these properties are only read after the Java process has started, not all JVM options can be set this way.

2. HOYA_JVM_OPTIONS environment variable to fix the JVM options.

3. A HOYA_CONF_DIR environment variable to set the hoya configuration directory

4. $HOYA_CONF_DIR/hoya-client.xml - client side options in a Hadoop XML file

5. $HOYA_CONF_DIR/log4j.properties -logging information.

6. One or more --conf <filename> values listed before the cluster name. This is a feature of the service launcher class itself.

7. Extra Hadoop properties set with -D name=value . These are applied after the hoya-client.xml file has been read and allow it to be overridden.

Proposed

1. Retain the existing configuration model

2. a --confdir attribute allows the configuration directory to be set directly. This would have to be picked up in the startup script(s), and the HOYA_CONFDIR env variable set from it (which will propagate the log4j settings as well as the hoya-client.xml file).

## Exit codes

Meaningful exit codes are critical to understanding why things fail -and for functional tests that use the scripts.

Hoya has a set of these built up from unit tests, with a plan to move some of the core error messages into Hadoop along with the base class service launcher itself.

These are [documented on github](https://github.com/hortonworks/hoya/blob/master/src/site/markdown/exitcodes.md):

All codes < 64 are considered to be "common across launched services" and raised by any entry point in Hoya (including the AM and other YARN services). Errors 64+ are Hoya-specific.

   
```

    /**

     * 0: success

     */

    int EXIT_SUCCESS                    =  0;

    

    /**

     * -1: generic "false" response. The operation worked but

     * the result was not true

     */

    int EXIT_FALSE                      = -1;

    

    /**

     * Exit code when a client requested service termination:

     */

    int EXIT_CLIENT_INITIATED_SHUTDOWN  =  1;

    

    /**

     * Exit code when targets could not be launched:

     */

    int EXIT_TASK_LAUNCH_FAILURE        =  2;

    

    /**

     * Exit code when an exception was thrown from the service:

     */

    int EXIT_EXCEPTION_THROWN           = 32;

    

    /**

     * Exit code when a usage message was printed:

     */

    int EXIT_USAGE                      = 33;

    

    /**

     * Exit code when something happened but we can't be specific:

     */

    int EXIT_OTHER_FAILURE              = 34;

    

    /**

     * Exit code when a control-C, kill -3, signal was picked up:

     */

                                  

    int EXIT_INTERRUPTED                = 35;

    

    /**

     * Exit code when the command line doesn't parse:, or

     * when it is otherwise invalid.

     */

    int EXIT_COMMAND_ARGUMENT_ERROR     = 36;

    

    /**

     * Exit code when the configurations in valid/incomplete:

     */

    int EXIT_BAD_CONFIGURATION          = 37;

    

    /**

     * Exit code when the configurations in valid/incomplete:

     */

    int EXIT_CONNECTIVTY_PROBLEM        = 38;

    

    /**

     * internal error:

     */

    int EXIT_INTERNAL_ERROR =       64;

    

    /**

     * Unimplemented feature:

     */

    int EXIT_UNIMPLEMENTED =        65;

  

    /**

     * service entered the failed state:

     */

    int EXIT_YARN_SERVICE_FAILED =  66;

  

    /**

     * service was killed:

     */

    int EXIT_YARN_SERVICE_KILLED =  67;

  

    /**

     * timeout on monitoring client:

     */

    int EXIT_TIMED_OUT =            68;

  

    /**

     * service finished with an error:

     */

    int EXIT_YARN_SERVICE_FINISHED_WITH_ERROR = 69;

  

    /**

     * the cluster is unknown:

     */

    int EXIT_UNKNOWN_HOYA_CLUSTER = 70;

  

    /**

     * the cluster is in the wrong state for that operation:

     */

    int EXIT_BAD_CLUSTER_STATE =    71;

  

    /**

     * A spawned master process failed 

     */

    int EXIT_MASTER_PROCESS_FAILED = 72;

    /**

     * The cluster failed -too many containers were

     * failing or some other threshold was reached

     */

    int EXIT_CLUSTER_FAILED = 73;

    

    /**

     * The cluster is live -and the requested operation

     * does not work if the cluster is running

     */

    int EXIT_CLUSTER_IN_USE = 74;

  

    /**

     * There already is a cluster of that name

     * when an attempt is made to create a new cluster

     */

    int EXIT_CLUSTER_EXISTS = 75;
```

A REST client could present the HTTP error codes directly, except:

1. It wouldn't translate error codes from different sections into specific failures. (e.g. 404 could have different meanings in parts of the tree)

2. The error code range on Unix is only 1 byte long: 256 values

3. The error code 0 is already taken to mean success

