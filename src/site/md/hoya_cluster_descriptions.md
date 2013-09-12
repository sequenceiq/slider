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


### Cluster-wide options

Cluster wide options are used to configure the application itself.

All options beginning with the prefix `site.` are converted into 
site XML options for the specific application (assuming they take a site XML 
configuration file).

## Roles

### Standard Role Options

### Env variables
 
 
All role options beginning with `env.` are automatically converted to
environment variables set for that container