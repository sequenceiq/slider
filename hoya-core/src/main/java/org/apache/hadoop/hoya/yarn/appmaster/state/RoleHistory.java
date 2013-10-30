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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class RoleHistory {
  protected static final Logger log =
    LoggerFactory.getLogger(RoleHistory.class);
  private long startTime;
  private long saveTime;
  private final NodeMap nodemap;
  private final int roleSize;

  private RoleStatus[] roles;
  private OutstandingRequestTracker outstandingRequests =
    new OutstandingRequestTracker();

  /**
   * For each role, lists nodes that are available for data-local allocation,
   ordered by more recently released - To accelerate node selection
   */
  private final List<NodeInstance> availableNodes[];

  public RoleHistory(int roleSize) {
    this.roleSize = roleSize;
    nodemap = new NodeMap(roleSize);
    availableNodes = new List[roleSize];
    for (int i = 0; i < roleSize; i++) {
      availableNodes[i] = new LinkedList<NodeInstance>();
    }
  }


  public void bootstrap() {
    log.info("Role history bootstrapped");
    startTime = System.currentTimeMillis();
  }
}
