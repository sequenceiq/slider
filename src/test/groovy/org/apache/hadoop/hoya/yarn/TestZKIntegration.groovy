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

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.Stat
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import java.util.concurrent.atomic.AtomicBoolean

@Commons
class TestZKIntegration extends Assert implements KeysForTests {

  
  protected MiniZooKeeperCluster zkCluster
  protected File baseDir
  private GString zkBindingString

  @Before
  void createCluster() {
    Configuration conf = new Configuration()
    zkCluster = new MiniZooKeeperCluster(conf)
    baseDir = File.createTempFile("zookeeper", ".dir")
    baseDir.delete()
    baseDir.mkdirs()
    log.info("ZK cluster with base dir $baseDir")
    int port = zkCluster.startup(baseDir)
    zkBindingString = "127.0.0.1:$port"
  }

  @After
  void teardown() {
    baseDir?.deleteDir()
  }

  @Test
  public void testIntegrationCreate() throws Throwable {
    ZKIntegration zki = createZKIntegrationInstance("cluster1")
    String userPath = ZKIntegration.mkHoyaUserPath(USERNAME)
    Stat stat = zki.stat(userPath)
    assert stat!=null
    log.info("User path $userPath has stat $stat")
  }

  def ZKIntegration createZKIntegrationInstance(String clusterName) {
    AtomicBoolean connectedFlag = new AtomicBoolean(false)
    ZKIntegration zki = ZKIntegration.newInstance(zkBindingString,
                                                  USERNAME,
                                                  clusterName,
                                                  false) {
      //connection callback
      synchronized (connectedFlag) {
        log.info("ZK binding callback received")
        connectedFlag.set(true)
        connectedFlag.notify()
      }
    }
    zki.init()
    //here the callback may or may not have occurred.
    waitForConnection(connectedFlag)
    log.info("ZK binding completed")
    return zki
  }

  public void waitForConnection(AtomicBoolean connectedFlag) {
    synchronized (connectedFlag) {
      if (!connectedFlag.get()) {
        log.info("waiting for ZK event")
        //wait a bit
        connectedFlag.wait(5000)
      }
    }
    assert connectedFlag.get()
  }

  @Test
  public void testListUserClustersWithoutAnyClusters() throws Throwable {
    ZKIntegration zki = createZKIntegrationInstance("")
    String userPath = ZKIntegration.mkHoyaUserPath(USERNAME)
    List<String> clusters = zki.clusters
    assert clusters.empty
  }
  
  @Test
  public void testListUserClustersWithOneCluster() throws Throwable {
    ZKIntegration zki = createZKIntegrationInstance("")
    String userPath = ZKIntegration.mkHoyaUserPath(USERNAME)
    String fullPath= zki.createPath(userPath, "/cluster-",
                   ZooDefs.Ids.OPEN_ACL_UNSAFE,
                   CreateMode.EPHEMERAL_SEQUENTIAL)
    log.info("Ephemeral path $fullPath")
    List<String> clusters = zki.clusters
    assert clusters.size()==1
    assert fullPath.endsWith(clusters[0])
  }

  @Test
  public void testListUserClustersWithTwoCluster() throws Throwable {
    ZKIntegration zki = createZKIntegrationInstance("")
    String userPath = ZKIntegration.mkHoyaUserPath(USERNAME)
    String c1 = createEphemeralChild(zki, userPath)
    log.info("Ephemeral path $c1")
    String c2 = createEphemeralChild(zki, userPath)
    log.info("Ephemeral path $c2")
    List<String> clusters = zki.clusters
    assert clusters.size() == 2
    assert (c1.endsWith(clusters[0]) && c1.endsWith(clusters[1]))  ||
         (c1.endsWith(clusters[1]) && c2.endsWith(clusters[0]))
  }

  def String createEphemeralChild(ZKIntegration zki, String userPath) {
    return zki.createPath(userPath, "/cluster-",
                          ZooDefs.Ids.OPEN_ACL_UNSAFE,
                          CreateMode.EPHEMERAL_SEQUENTIAL)
  }

}
