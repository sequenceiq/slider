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

package org.apache.hadoop.hoya.providers.accumulo;

import org.apache.accumulo.core.conf.Property;

/**
 * Mappings of config params to env variables for
 * custom -site.xml files to pick up
 *
 * A lot of these come from HConstants -the reason they have been copied
 * and pasted in here is to remove dependencies on HBase from
 * the Hoya Client and AM.
 */
public interface AccumuloConfigFileOptions {


  /**
   * quorum style, comma separated list of hostname:port values
   */
  String ZOOKEEPER_HOST = Property.INSTANCE_ZK_HOST.getKey();

  /**
   * URI to the filesystem
   */
  String INSTANCE_DFS_URI = Property.INSTANCE_DFS_URI.getKey();

  /**
   * Dir under the DFS URI
   */
  String INSTANCE_DFS_DIR = Property.INSTANCE_DFS_DIR.getKey();


  String INSTANCE_SECRET = Property.INSTANCE_SECRET.getKey();
  String MASTER_PORT_CLIENT = Property.MASTER_CLIENTPORT.getKey();
  String MASTER_PORT_CLIENT_DEFAULT = Property.MASTER_CLIENTPORT.getDefaultValue();
  
  String MONITOR_PORT_CLIENT = Property.MONITOR_PORT.getKey();
  String MONITOR_PORT_CLIENT_DEFAULT = Property.MONITOR_PORT.getDefaultValue();
  int MONITOR_PORT_CLIENT_INT = Integer.parseInt(MONITOR_PORT_CLIENT_DEFAULT);
  String TRACE_PORT_CLIENT = Property.TRACE_PORT.getKey();
  String TRACE_PORT_CLIENT_DEFAULT = Property.TRACE_PORT.getDefaultValue();

  String TSERV_PORT_CLIENT = Property.TSERV_CLIENTPORT.getKey();
  String TSERV_PORT_CLIENT_DEFAULT = Property.TSERV_CLIENTPORT.getDefaultValue();
  
}
