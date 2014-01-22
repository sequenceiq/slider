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
  
# Hoya Release 0.9.0

January 2014

This release is built against the Apache Hadoop 2.2.0 and hbase 0.96.0-hadoop2
artifacts. 


## Key changes

### CLI `monitor` action dropped

The monitioring logic has moved into the Application Master, where it will
ultimately monitor the liveness of the deployed service -for Hoya to react
to.

### Freeze command failes with ` EXIT_UNKNOWN_HOYA_CLUSTER = 70` on an unknown cluster

An attempt to freeze an unknown cluster now fails with the error code 70:

    hoya freeze no_such_cluster

### `freeze --force` force kills a hung AM

If an AM is hung, the normal freeze command fails; the `--force` option
avoids talking to the AM and just asks YARN to terminate the application.

    hoya freeze mycluster --force

### `env.MALLOC_ARENA_MAX` added to all roles by default

When the template roles are created, `env.MALLOC_ARENA_MAX=4`
is set for them. This addresses an issue with linux that can cause
containers to fail to start.


### cluster `original/` directory renamed `snapshot`

In earler releases, a copy of the supplied configuration directory was made
into the directory `~/.hoya/clusters/$CLUSTERNAME/original`. This
has caused a bit of confusion during diagnostic emails. We've renamed it
`snapshot` to make clear it is a snapshot of the original configuration directory.


