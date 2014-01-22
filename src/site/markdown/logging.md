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
  
# Logging

     The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL
      NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and
      "OPTIONAL" in this document are to be interpreted as described in
      RFC 2119.

## Logging for HoyaAM

In hoya-core/src/main/resources/org/apache/hoya/log4j.properties, we have:

#at debug this provides details on what is going on
log4j.logger.org.apache.hoya=DEBUG
#log4j.logger.org.apache.hoya.exec.RunLongLivedApp=ERROR

log4j.logger.org.apache.hadoop.security=DEBUG
log4j.logger.org.apache.hadoop.yarn.service.launcher=DEBUG
log4j.logger.org.apache.hadoop.yarn.service=DEBUG
log4j.logger.org.apache.hadoop.yarn.client=DEBUG

The DEBUG level is for troubleshooting purposes. It should be changed to INFO level when used in production.

## Logging for HBase

In hoya-core/src/main/resources/org/apache/hoya/providers/hbase/conf/log4j.properties,
DailyRollingFileAppender is used:

hbase.root.logger=INFO,DRFA
hbase.security.logger=INFO,DRFA

This would allow HBase master / region server logs to be rolled periodically so that
manageable amount of output is produced.
