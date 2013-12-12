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

 