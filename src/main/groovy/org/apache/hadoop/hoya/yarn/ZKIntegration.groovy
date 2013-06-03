/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hoya.yarn

import groovy.transform.CompileStatic
import groovy.util.logging.Commons
import org.apache.hadoop.hoya.tools.ZKCallback
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.data.ACL
import org.apache.zookeeper.data.Stat

import java.util.concurrent.atomic.AtomicBoolean

/**
 * ZK logic exists here, including the location of various services
 */
@Commons
@CompileStatic
class ZKIntegration implements Watcher {

/**
 * Base path for services
 */
  static String ZK_SERVICES = "services"
  /**
   * Base path for all Hoya references
   */
  static String ZK_HOYA = "hoya"
  static String ZK_USERS = "users"
  static String SVC_HOYA = "/" + ZK_SERVICES + "/" + ZK_HOYA
  static String SVC_HOYA_USERS = SVC_HOYA + "/" + ZK_USERS

  static List<String> ZK_USERS_PATH_LIST = [
      ZK_SERVICES,
      ZK_HOYA,
      ZK_USERS
  ]

  static int SESSION_TIMEOUT = 5000

  ZooKeeper zookeeper;
  final String username;
  final String clustername;
  final String userPath
  int sessionTimeout = SESSION_TIMEOUT;
/**
 flag to set to indicate that the user path should be created if
 it is not already there
 */
  final AtomicBoolean toInit = new AtomicBoolean(false)
  final Closure watchEventHandler
  private final String connection
  private final boolean canBeReadOnly;

  protected ZKIntegration(
      String zkConnection,
      String username,
      String clustername,
      boolean canBeReadOnly,
      Closure watchEventHandler
  ) throws IOException {
    this.username = username
    this.clustername = clustername
    this.watchEventHandler = watchEventHandler
    connection = zkConnection
    this.canBeReadOnly = canBeReadOnly
    this.userPath =mkHoyaUserPath(username)
  }

  public void init() {
    assert zookeeper == null
    log.debug("Binding ZK client to " + connection)
    zookeeper = new ZooKeeper(connection, sessionTimeout, this, canBeReadOnly);
  }

  /**
   * Create an instance bonded to the specific closure
   * @param zkConnection
   * @param username
   * @param clustername
   * @param canBeReadOnly
   * @param watchEventHandler
   * @return
   * @throws IOException
   */
  public static newInstance(
      String zkConnection,
      String username,
      String clustername,
      boolean canBeReadOnly,
      Closure watchEventHandler
  ) throws IOException {
    return new ZKIntegration(zkConnection, username, clustername, canBeReadOnly, watchEventHandler)
  }

  String getClusterPath() {
    return mkClusterPath(username, clustername);
  }

  boolean getConnected() {
    return zookeeper.state.connected
  }

  boolean getAlive() {
    return zookeeper.state.alive
  }

  ZooKeeper.States getState() {
    return zookeeper.state
  }

  Stat getClusterStat() {
    return stat(clusterPath)
  }

  boolean exists(String path) {
    return stat(path) != null
  }

  Stat stat(String path) {
    return zookeeper.exists(path, false)
  }

  /**
   * Event handler to notify of state events
   * @param event
   */
  @Override
  void process(WatchedEvent event) {
    log.debug("$event")
    maybeInit();
    watchEventHandler?.call(event)
  }

  private void maybeInit() {
    if (!toInit.getAndSet(true)) {
      log.debug('initing')
      //create the user path
      mkPath(ZK_USERS_PATH_LIST, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      //create the specific user
      createPath(userPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }
  }

  /**
   * Create a path under a parent, don't care if it already exists
   * As the path isn't returned, this isn't the way to create sequentially
   * numbered nodes.
   * @param parent parent dir. Must have a trailing / if entry!=null||empty 
   * @param entry entry -can be null or "", in which case it is not appended
   * @param acl
   * @param createMode
   * @return the path if created; null if not
   */
  String createPath(String parent,
                     String entry,
                     List<ACL> acl,
                     CreateMode createMode) {
    //initial create of full path
    assert acl != null
    assert acl.size() > 0
    assert parent
    String path = parent;
    if (entry) {
      path += entry;
    }
    try {
      log.debug("Creating ZK path $path")
      return zookeeper.create(path, null, acl, createMode)
    } catch (KeeperException.NodeExistsException ignored) {
      //node already there
      log.debug("node already present: $path")
      return null;
    }
  }

  /**
   * Recursive path create
   * @param path
   * @param data
   * @param acl
   * @param createMode
   */
  public void mkPath(List<String> paths,
                     List<ACL> acl,
                     CreateMode createMode) {
    String history = "/";
    paths.each { entry ->
      createPath(history, ((String)entry), acl, createMode)
      history += entry
      history += "/"
    }
  }

/**
 * Blocking enum of users
 * @return an unordered list of clusters under a user
 */
  List<String> getClusters() throws KeeperException, InterruptedException {
    zookeeper.getChildren(userPath, (Watcher) null)
  }

  /**
   * Best effort recursive delete <i>one level down</i>; mostly for testing
   * @param path
   */
  void recursiveDelete(String path) {
    List<String> children = zookeeper.getChildren(path,(Watcher) null)
    children.each {
    }
  }

  /**
   * Delete a node, does not throw an exception if the path is not fond
   * @param path path to delete
   * @return true if the path could be deleted, false if there was no node to delete 
   * 
   */
  boolean delete(String path) {
    try {
      zookeeper.delete(path, -1)
      return true
    } catch (KeeperException.NoNodeException ignored) {
      return false
    }
  }
  
/**
 * Build the path to a cluster; exists once the cluster has come up.
 * Even before that, a ZK watcher could wait for it.
 * @param username user
 * @param clustername name of the cluster
 * @return a strin
 */
  static String mkClusterPath(String username, String clustername) {
    return mkHoyaUserPath(username) + "/" + clustername
  }
/**
 * Build the path to a cluster; exists once the cluster has come up.
 * Even before that, a ZK watcher could wait for it.
 * @param username user
 * @param clustername name of the cluster
 * @return a strin
 */
  static String mkHoyaUserPath(String username) {
    return SVC_HOYA_USERS + "/" + username
  }

/**
 * Create a ZK watcher callback
 * @param closure
 * @return
 */
  static ZKCallback watcher(Closure closure) {
    return new ZKCallback() {
      @Override
      void process(WatchedEvent event) {
        closure(event)
      }
    }


  }


}
