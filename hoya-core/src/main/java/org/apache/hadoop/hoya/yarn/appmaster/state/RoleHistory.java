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
  private NodeMap nodemap;
  private int roleSize;
  private boolean dirty;

  private RoleStatus[] roles;
  private OutstandingRequestTracker outstandingRequests =
    new OutstandingRequestTracker();

  /**
   * For each role, lists nodes that are available for data-local allocation,
   ordered by more recently released - To accelerate node selection
   */
  private List<NodeInstance> availableNodes[];

  public RoleHistory(int roleSize) {
    reset(roleSize);

  }


  public void reset(int roleSize) {
    this.roleSize = roleSize;
    nodemap = new NodeMap(roleSize);
    availableNodes = new List[roleSize];
    for (int i = 0; i < roleSize; i++) {
      availableNodes[i] = new LinkedList<NodeInstance>();
    }
  }
  

  public long getStartTime() {
    return startTime;
  }

  public long getSaveTime() {
    return saveTime;
  }

  public void setSaveTime(long saveTime) {
    this.saveTime = saveTime;
  }

  public int getRoleSize() {
    return roleSize;
  }

  public boolean isDirty() {
    return dirty;
  }

  public void setDirty(boolean dirty) {
    this.dirty = dirty;
  }

  public void saved(long timestamp) {
    dirty = false;
    saveTime = timestamp;
  }
  
  public RoleStatus[] getRoles() {
    return roles;
  }

  public NodeMap getNodemap() {
    return nodemap;
  }

  public OutstandingRequestTracker getOutstandingRequests() {
    return outstandingRequests;
  }


  /**
   * Handler for bootstrap event
   */
  public void onBootstrap() {
    log.info("Role history bootstrapped");
    startTime = System.currentTimeMillis();
  }

  /**
   * Handle the thaw process <i>after the history has been rebuilt</i>
   */
  public void onThaw() {
    
  }
}
