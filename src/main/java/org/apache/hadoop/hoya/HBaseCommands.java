/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hoya;

/*
Usage: hbase <command>
where <command> an option from one of these categories:

DBA TOOLS
  shell            run the HBase shell
  hbck             run the hbase 'fsck' tool
  hlog             write-ahead-log analyzer
  hfile            store file analyzer
  zkcli            run the ZooKeeper shell

PROCESS MANAGEMENT
  master           run an HBase HMaster node
  regionserver     run an HBase HRegionServer node
  zookeeper        run a Zookeeper server
  rest             run an HBase REST server
  thrift           run the HBase Thrift server
  thrift2          run the HBase Thrift2 server
  avro             run an HBase Avro server

PACKAGE MANAGEMENT
  classpath        dump hbase CLASSPATH
  version          print the version


 */
public interface HBaseCommands {
  /** {@value */
  String MASTER = "master";
  
  /** {@value */
  String REGION_SERVER = "regionserver";

  String VERSION = "version";

  String ACTION_START = "start";
  String ACTION_STOP = "stop";

  /**
   * Config directory : {@value}
   */
  String ARG_CONFIG = "--config";
}
