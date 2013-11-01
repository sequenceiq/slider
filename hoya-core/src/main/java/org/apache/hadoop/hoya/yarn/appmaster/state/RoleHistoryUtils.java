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

package org.apache.hadoop.hoya.yarn.appmaster.state;

import org.apache.hadoop.hoya.exceptions.HoyaRuntimeException;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.hoya.avro.NodeAddress;

public class RoleHistoryUtils {
  public static NodeAddress addressOf(Container container) {
    NodeId nodeId = container.getNodeId();
    if (nodeId== null) {
      throw new HoyaRuntimeException("Container has no node ID: %s",
         HoyaUtils.containerToString(container));
    }
    return new NodeAddress(nodeId.getHost(), nodeId.getPort());
  }
  
  public static NodeAddress addressOf(NodeId nodeId) {
    return new NodeAddress(nodeId.getHost(), nodeId.getPort());
  }

  public static String toString(NodeAddress addr) {
    return addr.getHost() + ":" + addr.getPort();
  }

  /**
   * Decrement a value but hold it at zero. Usually a sanity check
   * on counters tracking outstanding operations
   * @param val value
   * @return decremented value
   */
  public static int decToFloor(int val) {
    int v = val-1;
    if (v < 0) {
      v = 0;
    }
    return v;
  }
  
}
