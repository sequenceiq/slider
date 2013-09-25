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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hoya.HoyaKeys;
import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.api.ClusterNode;
import org.apache.hadoop.hoya.exceptions.HoyaInternalStateException;
import org.apache.hadoop.hoya.exceptions.NoSuchNodeException;
import org.apache.hadoop.hoya.providers.ProviderRole;
import org.apache.hadoop.hoya.tools.ConfigHelper;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * The model of all the ongoing state of a Hoya AM.
 *
 * concurrency rules: any method which begins with <i>build</i>
 * is not synchronized and intended to be used during
 * initialization.
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


  /**
   * The master node. This is a shared reference with the clusterStatus;
   * operations on it MUST be synchronised with that object
   */
  private ClusterNode masterNode;


  /**
   * Hash map of the containers we have
   */
  private final ConcurrentMap<ContainerId, ContainerInfo> activeContainers =
    new ConcurrentHashMap<ContainerId, ContainerInfo>();

  /**
   * Hash map of the containers we have released, but we
   * are still awaiting acknowledgements on. Any failure of these
   * containers is treated as a successful outcome
   */
  private final ConcurrentMap<ContainerId, Container> containersBeingReleased =
    new ConcurrentHashMap<ContainerId, Container>();
  
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

  private Map<ContainerId, ClusterNode> getStartingNodes() {
    return startingNodes;
  }

  private Map<ContainerId, ClusterNode> getCompletedNodes() {
    return completedNodes;
  }

  public Map<ContainerId, ClusterNode> getFailedNodes() {
    return failedNodes;
  }

  private Map<ContainerId, ClusterNode> getLiveNodes() {
    return liveNodes;
  }

  public ClusterDescription getClusterSpec() {
    return clusterSpec;
  }

  public ClusterDescription getClusterStatus() {
    return clusterStatus;
  }

  public void setClusterStatus(ClusterDescription clusterStatus) {
    this.clusterStatus = clusterStatus;
  }

  private void setClusterSpec(ClusterDescription clusterSpec) {
    this.clusterSpec = clusterSpec;
  }

  public void buildInstance(ClusterDescription clusterSpec,
                            Configuration siteConf,
                            List<ProviderRole> providerRoles) {


    // set the cluster specification
    setClusterSpec(clusterSpec);


    //build the role list
    for (ProviderRole providerRole : providerRoles) {
      buildRole(providerRole);
    }
    //then pick up the requirements
    buildRoleRequirementsFromClusterSpec();

    //copy into cluster status. 
    ClusterDescription clusterStatus = ClusterDescription.copy(clusterSpec);
    Set<String> confKeys = ConfigHelper.sortedConfigKeys(siteConf);

//     Add the client properties
    for (String key : confKeys) {
      String val = siteConf.get(key);
      log.debug("{}={}", key, val);
      clusterStatus.clientProperties.put(key, val);
    }

    clusterStatus.state = ClusterDescription.STATE_CREATED;
    clusterStatus.startTime = System.currentTimeMillis();
    if (0 == clusterStatus.createTime) {
      clusterStatus.createTime = clusterStatus.startTime;
    }
    clusterStatus.statusTime = System.currentTimeMillis();
    clusterStatus.state = ClusterDescription.STATE_LIVE;

    //set the app state to this status
    setClusterStatus(clusterStatus);
    //now do an update, which 
  }

  public synchronized void updateClusterSpec(ClusterDescription cd) {
    setClusterSpec(cd);

    //propagate info from cluster, which is role table

    Map<String, Map<String, String>> roles = getClusterSpec().roles;
    getClusterStatus().roles = HoyaUtils.deepClone(roles);
    getClusterStatus().updateTime = System.currentTimeMillis();
    buildRoleRequirementsFromClusterSpec();


  }

  private void buildRoleRequirementsFromClusterSpec() {
    //now update every role's desired count.
    //if there are no instance values, that role count goes to zero
    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
      int currentDesired = roleStatus.getDesired();
      String role = roleStatus.getName();
      int desiredInstanceCount =
        getClusterSpec().getDesiredInstanceCount(role, -1);
      if (currentDesired != desiredInstanceCount) {
        log.info("Role {} flexed from {} to {}", role, currentDesired,
                 desiredInstanceCount);
        roleStatus.setDesired(desiredInstanceCount);
      }
    }
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
   * build up the special master node, which lives
   * in the live node set but has a lifecycle bonded to the AM
   * @param hostname hostname the master is running on
   * @param containerId the AM master
   */
  public void buildMasterNode(String hostname, ContainerId containerId) {
    masterNode = new ClusterNode(hostname);
    masterNode.containerId = containerId;
    masterNode.role = HoyaKeys.ROLE_MASTER;
    masterNode.uuid = UUID.randomUUID().toString();
    //it is also added to the set of live nodes
    getLiveNodes().put(containerId, masterNode);
  }

  /**
   * Note that the master node has been launched,
   * though it isn't considered live until any forked
   * processes are running
   */
  public void noteMasterNodeLaunched() {
    addLaunchedContainer(masterNode.containerId, masterNode);
  }

  /**
   * AM declares ourselves live in the cluster description.
   * This is meant to be triggered from the callback
   * indicating the spawned process is up and running.
   */
  public void noteMasterNodeLive() {
    masterNode.state = ClusterDescription.STATE_LIVE;
  }

  public ClusterNode getMasterNode() {
    return masterNode;
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
  @Deprecated
  public ConcurrentMap<ContainerId, ContainerInfo> getActiveContainers() {
    return activeContainers;
  }

  /**
   * Clone a list of active containers
   * @return the active containers at the time
   * the call was made
   */
  public synchronized List<ContainerInfo> cloneActiveContainerList() {
    Collection<ContainerInfo> values = activeContainers.values();
    return new ArrayList<ContainerInfo>(values);
  }


  
  public void addActiveContainer(ContainerInfo containerInfo) {
    activeContainers.putIfAbsent(containerInfo.getId(), containerInfo);
  }

  public ContainerInfo getActiveContainer(ContainerId id) {
    return activeContainers.get(id);
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
   * Get the {@link ClusterNode} details on a node
   * @param uuid the UUID
   * @return null if there is no such node
   * @throws NoSuchNodeException if the node cannot be found
   * @throws IOException IO problems
   */
  public synchronized  ClusterNode getLiveClusterNodeByUUID(String uuid) throws IOException, NoSuchNodeException {
    Collection<ClusterNode> nodes = getLiveNodes().values();
    for (ClusterNode node : nodes) {
      if (uuid.equals(node.uuid)) {
        return node;
      }
    }
    //at this point: no node
    throw new NoSuchNodeException(uuid);
  }

  /**
   * Get the {@link ClusterNode} details on a list of nodes.
   * Unknown nodes are not returned
   * <i>Important: the order of the results are undefined</i>
   * @param uuid the UUIDs
   * @return list of cluster nodes
   * @throws IOException IO problems
   */
  public List<ClusterNode> getLiveClusterNodesByUUID(String[] uuids) throws IOException {
    //first, a hashmap of those uuids is built up
    Set<String> uuidSet = new HashSet<String>(Arrays.asList(uuids));
    List<ClusterNode> nodes = new ArrayList<ClusterNode>(uuids.length);
    Collection<ClusterNode> clusterNodes = getLiveNodes().values();

    for (ClusterNode node : clusterNodes) {
      if (uuidSet.contains(node.uuid)) {
        nodes.add(node);
      }
    }
    //at this point: a possibly empty list of nodes
    return nodes;
  }
  
  /**
   * Enum all nodes by role. 
   * @param role role, or "" for all roles
   * @return a list of nodes, may be empty
   */
  public synchronized List<ClusterNode> enumLiveNodesInRole(String role) {
    List<ClusterNode> nodes = new ArrayList<ClusterNode>();
    Collection<ClusterNode> allClusterNodes = getLiveNodes().values();
    for (ClusterNode node : allClusterNodes) {
      if (role.isEmpty() || role.equals(node.role)) {
        nodes.add(node);
      }
    }
    return nodes;
  }


  /**
   * Build an instance map.
   * @return the map of RoleId -> count
   */
  private synchronized Map<String, Integer> createRoleToInstanceMap() {
    Map<String, Integer> map = new HashMap<String, Integer>();
    for (ClusterNode node : getLiveNodes().values()) {
      Integer entry = map.get(node.role);
      int current = entry != null ? entry : 0;
      current++;
      map.put(node.role, current);
    }
    return map;
  }

  /**
   * The containers we have released, but we
   * are still awaiting acknowledgements on. Any failure of these
   * containers is treated as a successful outcome
   */
  @Deprecated
  public ConcurrentMap<ContainerId, Container> getContainersBeingReleased() {
    return containersBeingReleased;
  }
  /**
   * Notification called just before the NM is asked to 
   * start a container
   * @param container container to start
   * @param clusterNode clusterNode structure
   */
  public void containerStartSubmitted(Container container,
                                      ClusterNode clusterNode) {
    clusterNode.state = ClusterDescription.STATE_SUBMITTED;
    clusterNode.containerId = container.getId();
    ContainerInfo containerInfo = new ContainerInfo(container);
    containerInfo.role = clusterNode.role;
    containerInfo.roleId = clusterNode.roleId;
    containerInfo.createTime = System.currentTimeMillis();

    getStartingNodes().put(container.getId(), clusterNode);
    activeContainers.putIfAbsent(container.getId(), containerInfo);

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
    role.incRequested();

    return request;
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
    ContainerInfo cinfo = activeContainers.get(containerId);
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
    activeContainers.remove(containerId);
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
   * handle completed node in the CD -move something from the live
   * server list to the completed server list
   * @param completed the node that has just completed
   */
  public synchronized void onCompletedNode(ContainerStatus completed) {

    //remove the node
    ContainerId id = completed.getContainerId();
    ClusterNode node = getLiveNodes().remove(id);
    if (node == null) {
      node = new ClusterNode();
      node.name = id.toString();
      node.containerId = id;
    }
    node.state = ClusterDescription.STATE_DESTROYED;
    node.exitCode = completed.getExitStatus();
    node.diagnostics = completed.getDiagnostics();
    getCompletedNodes().put(id, node);
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

  /**
   * Update the cluster description with anything interesting
   */
  public void refreshClusterStatus() {

    getClusterStatus().statusTime = System.currentTimeMillis();
    getClusterStatus().stats = new HashMap<String, Map<String, Integer>>();
    Map<String, Integer> instanceMap = createRoleToInstanceMap();
    if (log.isDebugEnabled()) {
      for (Map.Entry<String, Integer> entry : instanceMap.entrySet()) {
        log.debug("[%{}]: %{}", entry.getKey(), entry.getValue());
      }
    }
    getClusterStatus().instances = instanceMap;
    
    for (RoleStatus role : getRoleStatusMap().values()) {
      String rolename = role.getName();
      Integer count = instanceMap.get(rolename);
      if (count == null) {
        count = 0;
      } 
      int nodeCount = count;
      getClusterStatus().setActualInstanceCount(rolename, nodeCount);
      Map<String, Integer> stats = role.buildStatistics();
      getClusterStatus().stats.put(rolename, stats);
    }
  }
}
