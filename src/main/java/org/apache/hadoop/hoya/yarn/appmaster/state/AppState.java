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

import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.api.ClusterNode;
import org.apache.hadoop.hoya.exceptions.HoyaInternalStateException;
import org.apache.hadoop.hoya.providers.ProviderRole;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A class that contains all the ongoing state of a Hoya AM.
 * Keeping it isolated aids in testing and synchronizing access
 */
public class AppState {
  protected static final Logger log =
    LoggerFactory.getLogger(AppState.class);
  public static final String ROLE_UNKNOWN = "unknown";

  /**
   The cluster description published to callers
   This is used as a synchronization point on activities that update
   the CD, and also to update some of the structures that
   feed in to the CD
   */
  public ClusterDescription clusterSpec = new ClusterDescription();
  /**
   * This is the status, the live model
   */
  public ClusterDescription clusterStatus = new ClusterDescription();

  private final Map<Integer, RoleStatus> roleStatusMap =
    new HashMap<Integer, RoleStatus>();

  private final Map<String, ProviderRole> roles =
    new HashMap<String, ProviderRole>();

  private final ContainerTracker containerTracker = new ContainerTracker();

  /**
   *  This is the number of containers which we desire for HoyaAM to maintain
   */
  //private int desiredContainerCount = 0;

  /**
   * Counter for completed containers ( complete denotes successful or failed )
   */
  private final AtomicInteger numCompletedContainers = new AtomicInteger();

  /**
   *   Count of failed containers

   */
  private final AtomicInteger numFailedContainers = new AtomicInteger();

  /**
   * # of started containers
   */
  private final AtomicInteger startedContainers = new AtomicInteger();

  /**
   * # of containers that failed to start 
   */
  private final AtomicInteger startFailedContainers = new AtomicInteger();


  /**
   * Map of requested nodes. This records the command used to start it,
   * resources, etc. When container started callback is received,
   * the node is promoted from here to the containerMap
   */
  private final Map<ContainerId, ClusterNode> startingNodes =
    new ConcurrentHashMap<ContainerId, ClusterNode>();

  /**
   * List of completed nodes. This isn't kept in the CD as it gets too
   * big for the RPC responses. Indeed, we should think about how deep to get this
   */
  private final Map<ContainerId, ClusterNode> completedNodes
    = new ConcurrentHashMap<ContainerId, ClusterNode>();

  /**
   * Nodes that failed to start.
   * Again, kept out of the CD
   */
  private final Map<ContainerId, ClusterNode> failedNodes =
    new ConcurrentHashMap<ContainerId, ClusterNode>();

  /**
   * Map of containerID -> cluster nodes, for status reports.
   * Access to this should be synchronized on the clusterDescription
   */
  private final Map<ContainerId, ClusterNode> liveNodes =
    new ConcurrentHashMap<ContainerId, ClusterNode>();


  /**
   * Init phase
   * @param cd
   */
  public void init(ClusterDescription cd) {
    this.clusterSpec = cd;
  }
  
  
  public int getFailedCountainerCount() {
    return numFailedContainers.get();
  }

  /**
   * Increment the count and return the new value
   * @return the latest failed container count
   */
  public int incFailedCountainerCount() {
    return numFailedContainers.incrementAndGet();
  }

  public int getStartFailedCountainerCount() {
    return startFailedContainers.get();
  }

  /**
   * Increment the count and return the new value
   * @return the latest failed container count
   */
  public int incStartedCountainerCount() {
    return startedContainers.incrementAndGet();
  }

  public int getStartedCountainerCount() {
    return startedContainers.get();
  }

  /**
   * Increment the count and return the new value
   * @return the latest failed container count
   */
  public int incStartFailedCountainerCount() {
    return startFailedContainers.incrementAndGet();
  }

  public AtomicInteger getStartFailedContainers() {
    return startFailedContainers;
  }


  public Map<Integer, RoleStatus> getRoleStatusMap() {
    return roleStatusMap;
  }

  public Map<ContainerId, ClusterNode> getStartingNodes() {
    return startingNodes;
  }

  public Map<ContainerId, ClusterNode> getCompletedNodes() {
    return completedNodes;
  }

  public Map<ContainerId, ClusterNode> getFailedNodes() {
    return failedNodes;
  }

  public Map<ContainerId, ClusterNode> getLiveNodes() {
    return liveNodes;
  }

  public ClusterDescription getClusterSpec() {
    return clusterSpec;
  }

  public void setClusterSpec(ClusterDescription clusterSpec) {
    this.clusterSpec = clusterSpec;
  }

  public ClusterDescription getClusterStatus() {
    return clusterStatus;
  }

  public void setClusterStatus(ClusterDescription clusterStatus) {
    this.clusterStatus = clusterStatus;
  }

  /**
   * Add knowledge of a role.
   * This is a build-time operation that is not synchronized, and
   * should be used while setting up the system state -before servicing
   * requests.
   * @param providerRole role to add
   */
  public void buildRole(ProviderRole providerRole) {
    //build role status map
    roleStatusMap.put(providerRole.id,
                      new RoleStatus(providerRole));
    roles.put(providerRole.name, providerRole);
  }


  /**
   * Look up a role from its key -or fail 
   *
   * @param key key to resolve
   * @return the status
   * @throws YarnRuntimeException on no match
   */
  public RoleStatus lookupRoleStatus(int key) {
    RoleStatus rs = getRoleStatusMap().get(key);
    if (rs == null) {
      throw new YarnRuntimeException("Cannot find role for role key " + key);
    }
    return rs;
  }

  /**
   * Look up a role from its key -or fail 
   *
   * @param c container in a role
   * @return the status
   * @throws YarnRuntimeException on no match
   */
  public RoleStatus lookupRoleStatus(String name) {
    ProviderRole providerRole = roles.get(name);
    if (providerRole == null) {
      throw new YarnRuntimeException("Unknown role " + name);
    }
    return lookupRoleStatus(providerRole.id);
  }

  /**
   * Get all the active containers
   */
  public ConcurrentMap<ContainerId, ContainerInfo> getActiveContainers() {
    return containerTracker.getActiveContainers();
  }

  public void addActiveContainer(ContainerInfo containerInfo) {
    getActiveContainers().putIfAbsent(containerInfo.getId(), containerInfo);
  }

  public ContainerInfo getActiveContainer(ContainerId id) {
    return getActiveContainers().get(id);
  }

  /**
   * Create a clone of the list of live cluster nodes.
   * @return the list of nodes, may be empty
   */
  public synchronized List<ClusterNode> cloneLiveClusterNodeList() {
    List<ClusterNode> allClusterNodes;
    Collection<ClusterNode> values = getLiveNodes().values();
    allClusterNodes = new ArrayList<ClusterNode>(values);
    return allClusterNodes;
  }

  /**
   * The containers we have released, but we
   * are still awaiting acknowledgements on. Any failure of these
   * containers is treated as a successful outcome
   */
  public ConcurrentMap<ContainerId, Container> getContainersBeingReleased() {
    return containerTracker.getContainersBeingReleased();
  }

  /**
   * Note that a container has been submitted for release; update internal state
   * and mark the associated ContainerInfo released field to indicate that
   * while it is still in the active list, it has been queued for release.
   * @param id container ID
   * @throws HoyaInternalStateException if there is no container of that ID
   * on the active list
   */
  public synchronized void containerReleaseSubmitted(ContainerId id) throws
                                                                     HoyaInternalStateException {
    //look up the container
    ContainerInfo info = getActiveContainer(id);
    if (info == null) {
      throw new HoyaInternalStateException(
        "No active container with ID " + id.toString());
    }
    //verify that it isn't already released
    if (getContainersBeingReleased().containsKey(id)) {
      throw new HoyaInternalStateException(
        "Container %s already queued for release", id);
    }
    info.released = true;
    getContainersBeingReleased().put(id, info.container);
    RoleStatus role = lookupRoleStatus(info.roleId);
    role.incReleasing();
  }


  /**
   * Create a container request.
   * This can update internal state, such as the role request count
   * TODO: this is where role history information will be used for placement decisions -
   * @param role role
   * @param resource requirements
   * @return the container request to submit
   */
  public AMRMClient.ContainerRequest createContainerRequest(RoleStatus role,
                                                             Resource resource) {
    // setup requirements for hosts
    // using * as any host initially
    String[] hosts = null;
    String[] racks = null;
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(role.getPriority());
    AMRMClient.ContainerRequest request;
    request = new AMRMClient.ContainerRequest(resource,
                                              hosts,
                                              racks,
                                              pri,
                                              true);
   containerRequestSubmitted(role, request);

    return request;
  }


  /**
   * Note that a container request has been submitted.
   * @param role role of container
   * @param containerAsk request for RM
   */
  public void containerRequestSubmitted(RoleStatus role,
                                        AMRMClient.ContainerRequest containerAsk) {
    role.incRequested();
  }

  /**
   * add a launched container to the node map for status responss
   * @param id id
   * @param node node details
   */
  public void addLaunchedContainer(ContainerId id, ClusterNode node) {
    node.containerId = id;
    if (node.role == null) {
      log.warn("Unknown role for node {}", node);
      node.role = ROLE_UNKNOWN;
    }
    if (node.uuid == null) {
      node.uuid = UUID.randomUUID().toString();
      log.warn("creating UUID for node {}", node);
    }
    getLiveNodes().put(node.containerId, node);
  }

  public synchronized ContainerInfo onNodeManagerContainerStarted(ContainerId containerId) {
    incStartedCountainerCount();
    ContainerInfo cinfo = getActiveContainers().get(containerId);
    if (cinfo == null) {
      //serious problem
      log.error("Notification of container not in active containers start {}",
                containerId);
      return null;
    }
    cinfo.startTime = System.currentTimeMillis();
    ClusterNode node = getStartingNodes().remove(containerId);
    if (null == node) {
      log.error(
        "Creating a new node description for an unrequested node which" +
        "is known about");
      node = new ClusterNode(containerId.toString());
      node.role = cinfo.role;
    }
    node.state = ClusterDescription.STATE_LIVE;
    node.uuid = UUID.randomUUID().toString();
    addLaunchedContainer(containerId, node);
    return cinfo;
  }

  /**
   * update the application state after a failure to start a container.
   * This is perhaps where blacklisting could be most useful: failure
   * to start a container is a sign of a more serious problem
   * than a later exit.
   * 
   * -relayed from NMClientAsync.CallbackHandler 
   * @param containerId failing container
   * @param thrown what was thrown
   */
  public synchronized void onNodeManagerContainerStartFailed(ContainerId containerId,
                                                             Throwable thrown) {
    getActiveContainers().remove(containerId);
    incFailedCountainerCount();
    incStartFailedCountainerCount();
    ClusterNode node = getStartingNodes().remove(containerId);
    if (null != node) {
      if (null != thrown) {
        node.diagnostics = HoyaUtils.stringify(thrown);
      }
      getFailedNodes().put(containerId, node);
    }
  }
  /**
   * Return the percentage done that Hoya is to have YARN display in its
   * Web UI
   * @return an number from 0 to 100
   */
  public synchronized float getApplicationProgressPercentage() {
    float percentage = 0;
    int desired = 0;
    float actual = 0;
    for (RoleStatus role : getRoleStatusMap().values()) {
      desired += role.getDesired();
      actual += role.getActual();
    }
    if (desired == 0) {
      percentage = 100;
    } else {
      percentage = actual / desired;
    }
    return percentage;
  }
  
  
}
