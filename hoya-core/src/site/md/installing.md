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
  
# Installing and Running Hoya

Hoya deliberately avoids coming pre-packaged with all its JAR dependencies,
as re-using the existing Hadoop and YARN JARs avoids version conflict between
any Hoya-redistributed artifacts and those of the target cluster.

Instead: Hoya requires a local Hadoop installation, which it will re-use

In the absence of an automated installer, you can add the hoyac-core and jcommander
JARs to the lib directory under `$YARN_HOME`; in a tar-file installation
of hadoop this will be 
 