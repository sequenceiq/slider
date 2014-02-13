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
  
# todo list

First pass: 
* target HBase only
* exec .py start/stop scripts directly
* .py scripts return an error if they fail to start the hbase process
* ignore fact that hbase stays up on role termination

1. assume stack and hbase are installed.

1. support >1 role in AM with same launcher operation (just role
   name passed down.
   
1. Agent client to build up any params needed for agent

Site
   * URL to talk back to Agent Controller
   * path to package
   * path to hbase directory e.g `/share/hbase` or `~/apps/hbase-0.98.0/`
   key point: bin/hbase is underneath it.
   * path to agent (can be existing `app.home` property)
   
Role
   * name of role
   * path to .py file

1. Generate conf JSON file (client side for now)
   * load template
   * patch via code: load in JSON, edit, save.
   
1. short term
   py $something && sleep 600000

# Testing

1. Get a new sandbox 
or
1. modify the existing sandbox (keeping heap size values down)

1. provide a test config to work

1. write a test