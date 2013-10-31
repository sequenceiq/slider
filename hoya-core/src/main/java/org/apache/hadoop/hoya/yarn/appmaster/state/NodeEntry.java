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

/**
 * Information about the state of a role on a specific node instance.
 * No fields are synchronized; sync on the instance to work with it
 *
 The two fields `releasing` and `requested` are used to track the ongoing
 state of YARN requests; they do not need to be persisted across freeze/thaw
 cycles. They may be relevant across AM restart, but without other data
 structures in the AM, not enough to track what the AM was up to before
 it was restarted. The strategy will be to ignore unexpected allocation
 responses (which may come from pre-restart) requests, while treating
 unexpected container release responses as failures.

 The `active` counter is only decremented after a container release response
 has been received.
 
 Accesses are synchronized.
 */
public class NodeEntry {
  /**
   * Number of active nodes. Active includes starting as well as live
   */
  private int active;
  
  private int requested;
  private int releasing;
  private long lastUsed;
  
  /**
   * Is the node available for assignments.
   * @return true if there are no outstanding requests or role instances here
   * other than some being released.
   */
  public synchronized boolean isAvailable() {
    return (active - releasing) == 0 && (requested == 0);
  }

  /**
   * Return true if the node is not busy, and it
   * has not been used since the absolute time
   * @param absoluteTime time
   * @return true if the node could be cleaned up
   */
  public synchronized boolean notUsedSince(long absoluteTime) {
    return isAvailable() && lastUsed < absoluteTime;
  }

  /**
   * Number of active nodes. Active includes starting as well as live
   */
  public synchronized int getActive() {
    return active;
  }

  public synchronized void setActive(int active) {
    this.active = active;
  }

  public synchronized void incActive() {
    ++active;
  }

  public synchronized void decActive() {
    active = RoleHistoryUtils.decToFloor(active);
  }

  /**
   * no of requests made of this role of this node. If it goes above
   * 1 there's a problem
   */

  public int getRequested() {
    return requested;
  }

  /**
   * request a node: 
   */
  public synchronized void request() {
    ++requested;
    ++active;
  }

  public synchronized void requesteCompleted() {
    assert requested > 0;
    requested = RoleHistoryUtils.decToFloor(requested);
  }

  /**
   * No of instances in release state
   */
  public synchronized int getReleasing() {
    return releasing;
  }

  /**
   * Release an instance -which is no longer marked as active
   */
  public synchronized void release() {
    assert active > 0;
    releasing++;
    releasing = RoleHistoryUtils.decToFloor(releasing);
  }

  public synchronized void releaseCompleted() {
    assert releasing > 0;
    releasing = RoleHistoryUtils.decToFloor(releasing);
  }

  /**
   * Time last used.
   */
  public synchronized long getLastUsed() {
    return lastUsed;
  }

  public synchronized void setLastUsed(long lastUsed) {
    this.lastUsed = lastUsed;
  }
}
