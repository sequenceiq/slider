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

import java.util.HashMap;

public class NodeMap extends HashMap<String, NodeInstance> {

  private final int roleSize;


  public NodeMap(int roleSize) {
    this.roleSize = roleSize;
  }

  /**
   * Get the node instance for the specific node -creating it if needed
   * @param hostname node
   * @return the instance
   */
  public NodeInstance getOrCreate(String hostname) {
    NodeInstance node = get(hostname);
    if (node == null) {
      node = new NodeInstance(hostname, roleSize);
      put(hostname, node);
    }
    return node;
  }


  /**
   * Clone point
   * @return
   */
  @Override
  public Object clone() {
    return super.clone();
  }
}
