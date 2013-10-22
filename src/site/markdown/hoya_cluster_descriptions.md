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
  
# Hoya Cluster Descriptions



##  Hoya Options

These are options read by the Hoya Application Master; the program deployed
in the YARN cluster to start all the other roles.

When the AM is started, all entries in the cluster-wide  are loaded as Hadoop
configuration values.

Only those related to Hadoop, the filesystem in use, YARN and hoya will be
used; others are likely to be ignored.

## Cluster Options

Cluster wide options are used to configure the application itself.

These are specified at the command line with the `-O key=value` syntax

All options beginning with the prefix `site.` are converted into 
site XML options for the specific application (assuming they take a site XML 
configuration file).

Standard keys are defined in the class `org.apache.hadoop.hoya.api.OptionKeys`.

####  `cluster.app.version`

The version of the application. This is used to determine the paths
of binary files inside the .tar or `.tar.gz` file containing the
application to deploy. It *must* match the path inside that file
exactly, else the application will not run.

It can be specified on the command line in two ways:

    -O cluster.app.version=hbase-0.97.0-SNAPSHOT
    --version hbase-0.97.0-SNAPSHOT



####  `hoya.container.startup.delay`

This is the delay between spawning the service master process (such as the
HBase master) and requesting any containers for nodes in other cluster roles.

A long delay can ensure that the master service is up before any workers are up
-and also ensures that if the master cannot start for any reason, no effort
is wasted trying to start worker nodes on a failed cluster instance.

A short delay can bring up a cluster faster, especially one with idle resources. 

####  ``


####  ``





####  `hoya.test`

A boolean value to indicate this is a test run, not a production run. In this
mode Hoya opts to fail fast, rather than retry container deployments when
they fail. It is primarily used for internal tests.

####  `hoya.test.master.command`

list the single argument to invoke when starting the master role of the current provider.
This is used in testing to execute the "version" command, so as to verify that
the process starts and finishes successfully, without spawning a long-lived process.



    -O hoya.test.master.command=version


## Roles

### Standard Role Options

### Env variables
 
 
All role options beginning with `env.` are automatically converted to
environment variables set for that container

    --roleopt worker env.MALLOC_ARENA 4
