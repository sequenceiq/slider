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

import org.apache.hadoop.hoya.providers.ProviderRole;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hoya.avro.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The Role History.
 * 
 * Synchronization policy: all public operations are synchronized.
 * Protected methods are in place for testing -no guarantees are made.
 * 
 * Inner classes have no synchronization guarantees; they should be manipulated 
 * in these classes and not externally
 * 
 */
public class RoleHistory {
  protected static final Logger log =
    LoggerFactory.getLogger(RoleHistory.class);
  private final List<ProviderRole> providerRoles;
  private final Map<String, ProviderRole> providerRoleMap =
    new HashMap<String, ProviderRole>();
  private long startTime;
  private long saveTime;
  private NodeMap nodemap;
  private int roleSize;
  private boolean dirty;

  private RoleStatus[] roleStats;
  private OutstandingRequestTracker outstandingRequests =
    new OutstandingRequestTracker();

  /**
   * For each role, lists nodes that are available for data-local allocation,
   ordered by more recently released - To accelerate node selection
   */
  private List<NodeInstance> availableNodes[];

  public RoleHistory(List<ProviderRole> providerRoles) {
    this.providerRoles = providerRoles;
    roleSize = providerRoles.size();
    for (ProviderRole providerRole : providerRoles) {
      providerRoleMap.put(providerRole.name, providerRole);
    }
    reset();

  }


  /**
   * Reset the variables -this does not adjust the fixed attributes
   * of the history
   */
  protected void reset() {

    nodemap = new NodeMap(roleSize);
    availableNodes = new List[roleSize];

    resetAvailableNodeLists();
    outstandingRequests = new OutstandingRequestTracker();

    roleStats = new RoleStatus[roleSize];

    for (ProviderRole providerRole : providerRoles) {
      int index = providerRole.id;
      if (index > roleSize || index < 0) {
        throw new ArrayIndexOutOfBoundsException("Provider " + providerRole
                                                 + " id is out of range");
      }
      if (roleStats[index]!=null) {
        throw new ArrayIndexOutOfBoundsException(
          providerRole.toString() + " id duplicates that of " + roleStats[index]);
      }
      roleStats[index] = new RoleStatus(providerRole);
    }

  }

  private void resetAvailableNodeLists() {
    for (int i = 0; i < roleSize; i++) {
      availableNodes[i] = new LinkedList<NodeInstance>();
    }
  }


  /**
   * Reset the variables -this does not adjust the fixed attributes
   * of the history
   */
  public synchronized void prepareForReading(int roleCountInSource) throws
                                                                    IOException {
    reset();
    if (roleCountInSource!=roleSize) {
      throw new IOException("Number of roles in source " + roleCountInSource
                            + " does not match the expected number of " +
                            roleSize);
    }
  }
  public synchronized long getStartTime() {
    return startTime;
  }

  public synchronized long getSaveTime() {
    return saveTime;
  }

  public synchronized void setSaveTime(long saveTime) {
    this.saveTime = saveTime;
  }

  public int getRoleSize() {
    return roleSize;
  }

  public synchronized boolean isDirty() {
    return dirty;
  }

  public synchronized void setDirty(boolean dirty) {
    this.dirty = dirty;
  }

  public synchronized void saved(long timestamp) {
    dirty = false;
    saveTime = timestamp;
  }

  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  public List<ProviderRole> getProviderRoles() {
    return providerRoles;
  }

  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  public RoleStatus[] getRoleStats() {
    return roleStats;
  }

  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  protected NodeMap getNodemap() {
    return nodemap;
  }

  /**
   * Get a clone of the nodemap.
   * The instances inside are not cloned
   * @return the map
   */
  public synchronized NodeMap cloneNodemap() {
    return (NodeMap) nodemap.clone();
  }

  /**
   * Get the node instance for the specific node -creating it if needed
   * @param nodeAddr node
   * @return the instance
   */
  public NodeInstance getOrCreateNodeInstance(NodeAddress nodeAddr) {
    return nodemap.getOrCreate(nodeAddr);
  }
  
  /**
   * Get current time. overrideable for test subclasses
   * @return current time in millis
   */
  protected long now() {
    return System.currentTimeMillis();
  }

  /**
   * size of cluster
   * @return number of entries
   */
  public int size() {
    return nodemap.size();
  }

  /**
   * Garbage collect the structure -this will dropp
   * all nodes that have been inactive since the (relative) age
   * @param age relative age
   */
  public void gc(long age) {
    long absoluteTime = now() - age;
    purgeUnusedEntries(absoluteTime);
  }


  /**
   * purge the history of
   * all nodes that have been inactive since the absolute time
   * @param absoluteTime time
   */
  public synchronized void purgeUnusedEntries(long absoluteTime) {
    Iterator<Map.Entry<NodeAddress,NodeInstance>> iterator =
      nodemap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<NodeAddress, NodeInstance> entry = iterator.next();
      NodeAddress addr = entry.getKey();
      NodeInstance ni = entry.getValue();
      if (!ni.purgeUnusedEntries(absoluteTime)) {
        iterator.remove();
      }
    }
  }

  /**
   * Handler for bootstrap event
   */
  public void onBootstrap() {
    log.info("Role history bootstrapped");
    startTime = System.currentTimeMillis();
  }

  /**
   * Handle the thaw process <i>after the history has been rebuilt</i>,
   * and after any gc/purge
   */
  public synchronized void onThaw() {
    resetAvailableNodeLists();
    // build the list of available nodes
    for (Map.Entry<NodeAddress, NodeInstance> entry : nodemap
      .entrySet()) {
      NodeAddress addr = entry.getKey();
      NodeInstance ni = entry.getValue();
      for (int i = 0; i < roleSize; i++) {
        if (ni.isAvailable(i)) {
          availableNodes[i].add(ni);
        }
      }
    }
    // sort the resulting arrays
    for (int i = 0; i < roleSize; i++) {
      Collections.sort(availableNodes[i], new NodeInstance.newerThan(i));
    }
  }
  
  public synchronized void onAMRestart() {
    //TODO once AM restart is implemented and we know what to expect
  }

  /**
   * Find a node for use
   * @param role role
   * @return the instance, or null for none
   */
  protected synchronized NodeInstance findNodeForNewInstance(int role) {
    NodeInstance nodeInstance;
    List<NodeInstance> targets = availableNodes[role];
    if (targets.isEmpty()) {
      nodeInstance = null;
    } else {
      nodeInstance = targets.remove(0);
    } 
    return nodeInstance;
  }

  /**
   * Request an instance on a given node.
   * An outstanding request is created & tracked, with the 
   * relevant node entry for that role updated.
   *
   * The role status entries will also be tracked
   * 
   * Returns the request that is now being tracked.
   * If the node instance is not null, it's details about the role is incremented
   *
   *
   * @param node node to target or null for "any"
   * @param role role to request
   * @return the container priority
   */
  public synchronized AMRMClient.ContainerRequest requestInstanceOnNode(
    NodeInstance node, int role, Resource resource) {
    OutstandingRequest outstanding = outstandingRequests.addRequest(node, role);
    roleStats[role].incRequested();
    return outstanding.buildContainerRequest(resource);
  }
  
  public synchronized AMRMClient.ContainerRequest requestNode(int role, Resource resource) {
    NodeInstance node = findNodeForNewInstance(role);
     return requestInstanceOnNode(node, role,resource);
  }
}
