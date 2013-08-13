/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hoya.providers.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hoya.api.ClusterKeys;
import org.apache.hadoop.hoya.exceptions.HoyaException;
import org.apache.hadoop.hoya.providers.ClusterBuilder;

import java.util.HashMap;
import java.util.Map;

public class HBaseClusterBuilder extends HBaseClusterCore implements
                                                          ClusterBuilder {

  public static final String ERROR_UNKNOWN_ROLE = "Unknown role ";

  public HBaseClusterBuilder(Configuration conf) {
    super(conf);
  }

  /**
   * Create the default cluster role instance for a named
   * cluster role; 
   *
   * @param rolename role name
   * @return a node that can be added to the JSON
   */
  @Override
  public Map<String, String> createDefaultClusterRole(String rolename) throws
                                                                       HoyaException {
    Map<String, String> rolemap = new HashMap<String, String>();
    rolemap.put(ClusterKeys.NAME, rolename);
    String heapSize;
    String infoPort;
    if (rolename.equals(HBaseClusterCore.ROLE_WORKER)) {
      heapSize = DEFAULT_HBASE_WORKER_HEAP;
      infoPort = DEFAULT_HBASE_WORKER_INFOPORT;
    } else if (rolename.equals(HBaseClusterCore.ROLE_MASTER)) {
      heapSize = DEFAULT_HBASE_MASTER_HEAP;
      infoPort = DEFAULT_HBASE_MASTER_INFOPORT;
    } else {
      throw new HoyaException(ERROR_UNKNOWN_ROLE + rolename);
    }
    rolemap.put(ClusterKeys.INFO_PORT, infoPort);
    rolemap.put(ClusterKeys.HEAP_SIZE, heapSize);
    return rolemap;
  }
}
