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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.HoyaKeys;
import org.apache.hadoop.hoya.avro.RoleHistoryWriter;
import org.apache.hadoop.hoya.providers.ProviderRole;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
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
  private FileSystem filesystem;
  private Path historyPath;
  private RoleHistoryWriter historyWriter = new RoleHistoryWriter();

  private OutstandingRequestTracker outstandingRequests =
    new OutstandingRequestTracker();

  /**
   * For each role, lists nodes that are available for data-local allocation,
   ordered by more recently released - To accelerate node selection
   */
  private LinkedList<NodeInstance> availableNodes[];

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
    availableNodes = new LinkedList[roleSize];

    resetAvailableNodeLists();
    outstandingRequests = new OutstandingRequestTracker();
    RoleStatus[] roleStats;

    roleStats = new RoleStatus[roleSize];

    for (ProviderRole providerRole : providerRoles) {
      int index = providerRole.id;
      if (index >= roleSize || index < 0) {
        throw new ArrayIndexOutOfBoundsException("Provider " + providerRole
                                                 + " id is out of range");
      }
      if (roleStats[index] != null) {
        throw new ArrayIndexOutOfBoundsException(
          providerRole.toString() + " id duplicates that of " +
          roleStats[index]);
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

  /**
   * Get the total size of the cluster -the number of NodeInstances
   * @return a count
   */
  public int getClusterSize() {
    return nodemap.size();
  }
  
    public synchronized boolean isDirty() {
    return dirty;
  }

  public synchronized void setDirty(boolean dirty) {
    this.dirty = dirty;
  }

  /**
   * Tell the history that it has been saved; marks itself as clean
   * @param timestamp timestamp -updates the savetime field
   */
  public synchronized void saved(long timestamp) {
    dirty = false;
    saveTime = timestamp;
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
   * @param nodeAddr node address
   * @return the instance
   */
  public NodeInstance getOrCreateNodeInstance(String hostname) {
    //convert to a string
    return nodemap.getOrCreate(hostname);
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
   * Mark ourselves as dirty
   */
  public void touch() {
    setDirty(true);
  }


  /**
   * purge the history of
   * all nodes that have been inactive since the absolute time
   * @param absoluteTime time
   */
  public synchronized void purgeUnusedEntries(long absoluteTime) {
    Iterator<Map.Entry<String ,NodeInstance>> iterator =
      nodemap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, NodeInstance> entry = iterator.next();
      NodeInstance ni = entry.getValue();
      if (!ni.purgeUnusedEntries(absoluteTime)) {
        iterator.remove();
      }
    }
  }

  /**
   * Get the path used for history files
   * @return the directory used for history files
   */
  public Path getHistoryPath() {
    return historyPath;
  }

  /**
   * Create the filename for the history
   * @param time time value
   * @return a filename such that later filenames sort later in the directory
   */
  private Path createHistoryFilename(long time) {
    String filename = String.format(Locale.ENGLISH,
                                    HoyaKeys.HISTORY_FILENAME_PATTERN,
                                    time);
    Path path = new Path(historyPath, filename);
    return path;
  }

  public Path saveHistory(long time) throws IOException {
    Path filename = createHistoryFilename(time);
    saveTime = time;
    setDirty(false);
    historyWriter.write(filesystem, filename, true, this, time);
    return filename;
  }

  /**
   * Start up
   * @param fs filesystem 
   * @param historyDir path in FS for history
   */
  public void onStart(FileSystem fs, Path historyDir) {
    this.filesystem = fs;
    this.historyPath = historyDir;
    onBootstrap();
    }
  
  /**
   * Handler for bootstrap event
   */
  public void onBootstrap() {
    log.info("Role history bootstrapped");
    startTime = now();
  }

  /**
   * Handle the thaw process <i>after the history has been rebuilt</i>,
   * and after any gc/purge
   */
  public synchronized void onThaw() {
    resetAvailableNodeLists();
    // build the list of available nodes
    for (Map.Entry<String, NodeInstance> entry : nodemap
      .entrySet()) {
      NodeInstance ni = entry.getValue();
      for (int i = 0; i < roleSize; i++) {
        NodeEntry nodeEntry = ni.get(i);
        if (nodeEntry != null && nodeEntry.isAvailable()) {
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
    return outstanding.buildContainerRequest(resource);
  }
  
  public synchronized AMRMClient.ContainerRequest requestNode(int role, Resource resource) {
    NodeInstance node = findNodeForNewInstance(role);
     return requestInstanceOnNode(node, role,resource);
  }


  /**
   * Find a node for release; algorithm may make its own
   * decisions on which to release
   * @param role role index
   * @return a node or null if none was found
   */
  public NodeInstance findNodeForRelease(int role) {
    NodeInstance node = findNodeForRelease(role, 2);
    if (node != null) {
      return node;
    } else {
      return findNodeForRelease(role, 1);
    }
  }
  
  public synchronized NodeInstance findNodeForRelease(int role, int limit) {
    //find a node

    for (NodeInstance ni : nodemap.values()) {
      NodeEntry nodeEntry = ni.get(role);
      if (nodeEntry !=null && nodeEntry.getActive()>= limit) {
        return ni;
      }
    }
    return null;
  }


  /**
   * Get the node entry of a container
   * @param container
   * @return
   */
  public NodeEntry getOrCreateNodeEntry(Container container) {
    NodeInstance node = getOrCreateNodeInstance(container);
    return node.getOrCreate(ContainerPriority.extractRole(container));
  }

  /**
   * Get the node instance of a container -always returns something
   * @param container container to look up
   * @return a (possibly new) node instance
   */
  public NodeInstance getOrCreateNodeInstance(Container container) {
    String hostname = RoleHistoryUtils.hostnameOf(container);
    return nodemap.getOrCreate(hostname);
  }

  /**
   * Get the node instance of a an address if defined
   * @param addr address
   * @return a node instance or null
   */
  public NodeInstance getExistingNodeInstance(String hostname) {
    return nodemap.get(hostname);
  }

  
  
  /**
   * A container has been allocated on a node -update the data structures
   * @param container container
   */
  public void onContainerAllocated(Container container) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    nodeEntry.starting();
  }

  /**
   * Event: a container start has been submitter
   * @param container container being started
   * @param instance instance bound to the container
   */
  public void onContainerStartSubmitted(Container container,
                                        RoleInstance instance) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    int role = ContainerPriority.extractRole(container);
    // any actions we want here
  }

  /**
   * Container start event
   * @param container
   */
  public void onContainerStarted(Container container) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    nodeEntry.startCompleted();
  }

  /**
   * A container failed to start: update the node entry state
   * and return the container to the queue
   * @param container container that failed
   * @return true if the node was queued
   */
  public boolean onNodeManagerContainerStartFailed(Container container) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    boolean available = nodeEntry.startFailed();
    return maybeQueueNode(container, nodeEntry, available);
  }

  /**
   * A container release request was issued
   * @param container container submitted
   */
  public void onContainerReleaseSubmitted(Container container) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    nodeEntry.release();
  }

  /**
   * App state notified of a container completed 
   * @param container completed container
   * @return true if the node was queued
   */
  public boolean onReleaseCompleted(Container container) {
    return markContainerFinished(container, true);
  }

  /**
   * App state notified of a container completed -but as
   * it wasn't being released it is marked as failed
   * @param container completed container
   * @return true if the node was queued
   */
  public boolean onFailedNode(Container container) {
    return markContainerFinished(container, false);
  }

  /**
   * Mark a container finished; if it was released then that is treated
   * differently.
   *
   * @param container completed container
   * @param wasReleased was the container released?
   * @return true if the node was queued

   */
  protected synchronized boolean markContainerFinished(Container container,
                                                       boolean wasReleased) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    boolean available = nodeEntry.containerCompleted(wasReleased);
    return maybeQueueNode(container, nodeEntry, available);
  }

  /**
   * If the node is marked as available; queue it for assignments, mark
   * this structure as dirty
   * @param container completed container
   * @param nodeEntry node
   * @param available available flag
   * @return true if the node was queued
   */
  private boolean maybeQueueNode(Container container,
                              NodeEntry nodeEntry,
                              boolean available) {
    if (available) {
      //node is free
      nodeEntry.setLastUsed(now());
      NodeInstance ni = getOrCreateNodeInstance(container);
      int roleId = ContainerPriority.extractRole(container);
      log.debug("Node {} is now available for role id {}", ni, roleId);
      availableNodes[roleId].addFirst(ni);
      touch();
    }
    return available;
  }

  /**
   * Print the history to the log. This is for testing and diagnostics 
   */
  public synchronized void dump() {
    for (ProviderRole role : providerRoles) {
      log.info(role.toString());
      log.info("  available: " + availableNodes[role.id].size()
               + " " + HoyaUtils.joinWithInnerSeparator(", ", availableNodes[role.id]));
    }

    log.info("Nodes in Cluster: {}", getClusterSize());
    for (NodeInstance node : nodemap.values()) {
      log.info(node.toFullString());
    }
  }


  /**
   * Get a clone of the available list
   * @param role role index
   * @return a clone of the list
   */
  @VisibleForTesting
  public List<NodeInstance> cloneAvailableList(int role) {
    return new LinkedList<NodeInstance>(availableNodes[role]);
  }
}
