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

package org.apache.hoya.providers.agent;

/*

 */
public interface AgentKeys {

  String PROVIDER_AGENT = "agent";
  String ROLE_NODE = "node";
  /**
   * {@value}
   */
  String CONF_FILE = "agent.conf";
  /**
   * {@value}
   */
  String REGION_SERVER = "regionserver";
  /**
   * What is the command for hbase to print a version: {@value}
   */
  String COMMAND_VERSION = "version";
  String ACTION_START = "start";
  String ACTION_STOP = "stop";
  /**
   * Config directory : {@value}
   */
  String ARG_CONFIG = "--config";
  /**
   * Template stored in the hoya classpath -to use if there is
   * no site-specific template
   * {@value}
   */
  String CONF_RESOURCE = "org/apache/hoya/providers/agent/conf/";
  /*  URL to talk back to Agent Controller*/
  String CONTROLLER_URL = "agent.controller.url";
  /**
   * The location of pre-installed agent path.
   * This can be also be dynamically computed based on Yarn installation of agent.
   */
  String PACKAGE_PATH = "agent.package.root";
  /**
   * The location of the script implementing the command.
   */
  String SCRIPT_PATH = "agent.script";
  /**
   * Execution home for the agent.
   */
  String APP_HOME = "app.home";
  /**
   * Name of the service.
   */
  String SERVICE_NAME = "app.name";
  /**
   * Optional log location for the application.
   */
  String SERVICE_LOG_PATH = "app.log.path";
  /**
   * Optional PID location for the application.
   */
  String SERVICE_PID_PATH = "app.pid.path";

  String COMMAND_JSON_FILENAME = "command.json";

  //hbase-site and hdfs-site parameters
  String DFS_NAMENODE_HTTPS_ADDRESS = "site.dfs.namenode.https-address";
  String DFS_NAMENODE_HTTP_ADDRESS = "site.dfs.namenode.http-address";
  String HBASE_ROOTDIR = "site.hbase.rootdir";
  String HBASE_STAGINGDIR = "site.hbase.stagingdir";
  String HBASE_ZNODE_PARENT = "site.zookeeper.znode.parent";
  String HBASE_RS_INFO_PORT = "site.hbase.regionserver.info.port";
  String HBASE_MASTER_INFO_PORT = "site.hbase.master.info.port";
  // Keys to replace json
  String COMMAND_KEY = "COMMAND";
  String CLUSTER_NAME_KEY = "CLUSTER_NAME";
  String HOST_NAME_KEY = "HOST_NAME";
  String COMMAND_ID_KEY = "COMMAND_ID";
  String SERVICE_NAME_KEY = "SERVICE_NAME";
  String ROLE_NAME_KEY = "ROLE_NAME";
  String HABSE_HOME_KEY = "HBASE_HOME";
  String PID_DIR_KEY = "PID_DIR";
  String LOG_DIR_KEY = "LOG_DIR";
  String REGION_SERVER_HEAP_SIZE_KEY = "REGION_SERVER_HEAP_SIZE";
  String MASTER_HEAP_SIZE_KEY = "MASTER_HEAP_SIZE";
  String GROUP_NAME_KEY = "GROUP_NAME";
  String USER_NAME_KEY = "USER_NAME";
  String NAMENODE_HTTPS_ADDRESS_KEY = "NAMENODE_HTTPS_ADDRESS";
  String NAMENODE_HTTP_ADDRESS_KEY = "NAMENODE_HTTP_ADDRESS";
  String DEFAULT_FS_KEY = "DEFAULT_FS";
  String HBASE_ROOT_DIR_KEY = "HBASE_ROOT_DIR";
  String HBASE_STAGING_DIR_KEY = "HBASE_STAGING_DIR";
  String HBASE_SUPERUSER_KEY = "HBASE_SUPERUSER";
  String ZK_CLIENT_PORT_KEY = "ZK_CLIENT_PORT";
  String ZK_HOSTS_KEY = "ZK_HOSTS";
  String ZK_NODE_PARENT_KEY = "ZK_NODE_PARENT";
  String REGSION_SERVER_INFO_PORT_KEY = "REGION_SERVER_INFO_PORT";
  String MASTER_INFO_PORT = "MASTER_INFO_PORT";
}


