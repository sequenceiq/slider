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
  
# Troubleshooting

Hoya can be tricky to start using, because it combines the need to set
up a YARN application, with the need to have an HBase configuration
that works


### Common problems

## Classpath for Hoya AM wrong

The Hoya Application Master, the "Hoya AM" builds up its classpath from
those JARs it has locally, and the JARS pre-installed on the classpath

This often surfaces in an exception that can be summarized as
"hadoop-common.jar is not on the classpath":

    Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/hadoop/util/ExitUtil$ExitException
    Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.util.ExitUtil$ExitException
      at java.net.URLClassLoader$1.run(URLClassLoader.java:202)
      at java.security.AccessController.doPrivileged(Native Method)
      at java.net.URLClassLoader.findClass(URLClassLoader.java:190)
      at java.lang.ClassLoader.loadClass(ClassLoader.java:306)
      at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:301)
      at java.lang.ClassLoader.loadClass(ClassLoader.java:247)
    Could not find the main class: org.apache.hadoop.yarn.service.launcher.ServiceLauncher.  Program will exit.


For ambari-managed deployments, we recommend the following

  
      <property>
        <name>yarn.application.classpath</name>
        <value>
          /etc/hadoop/conf,/usr/lib/hadoop/*,/usr/lib/hadoop/lib/*,/usr/lib/hadoop-hdfs/*,/usr/lib/hadoop-hdfs/lib/*,/usr/lib/hadoop-yarn/*,/usr/lib/hadoop-yarn/lib/*,/usr/lib/hadoop-mapreduce/*,/usr/lib/hadoop-mapreduce/lib/*
        </value>
      </property>

The `yarn-site.xml` file for the site will contain the relevant value.

### Configuring YARN
 
 
One configuration to aid debugging is tell the nodemanagers to
keep data for a short period after containers finish

    <!-- 10 minutes after a failure to see what is left in the directory-->
    <property>
      <name>yarn.nodemanager.delete.debug-delay-sec</name>
      <value>600</value>
    </property>

You can then retrieve logs by either the web UI, or by connecting to the
server (usually by `ssh`) and retrieve the logs from the log directory


We also recommend making sure that YARN kills processes

    <!--time before the process gets a -9 -->
    <property>
      <name>yarn.nodemanager.sleep-delay-before-sigkill.ms</name>
      <value>30000</value>
    </property>

 