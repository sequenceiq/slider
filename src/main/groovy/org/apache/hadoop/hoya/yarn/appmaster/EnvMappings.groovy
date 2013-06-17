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

package org.apache.hadoop.hoya.yarn.appmaster

/**
 * Mappings of config params to env variables for
 * custom -site.xml files to pick up
 * 
 * A lot of these come from HConstants -the reason they have been copied
 * and pasted in here is to remove dependencies on HBase from
 * the Hoya Client and AM.
 */
class EnvMappings {
  
  public static final String ENV_FS_DEFAULT_NAME = 'FS_DEFAULT_NAME';
  public static final String ENV_ZOOKEEPER_PATH = 'ZOOKEEPER_PATH';
  public static final String ENV_ZOOKEEPER_CONNECTION = 'ZOOKEEPER_CONNECTION';
  public static final String ENV_HBASE_OPTS = 'HBASE_OPTS';

  public static final String KEY_HBASE_ROOTDIR = "hbase.rootdir";
  
  public static final String KEY_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum" ;
   //HConstants.ZOOKEEPER_QUORUM;
  public static final String KEY_ZOOKEEPER_PORT = "hbase.zookeeper.property.clientPort" ;
  //HConstants.ZOOKEEPER_CLIENT_PORT;
  public static final String KEY_ZNODE_PARENT = "zookeeper.znode.parent";
  
  
  //public static final int DEFAULT_MASTER_PORT = 60000;

  public static final String KEY_HBASE_MASTER_PORT = "hbase.master.port";

  public static final int HBASE_ZK_PORT = 2181; // HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;


  public static final String KEY_REGIONSERVER_PORT = "hbase.regionserver.port";
  public static final String KEY_REGIONSERVER_INFO_PORT = "hbase.regionserver.info.port";
  

}
