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

package org.apache.hoya.yarn.providers.hbase

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.api.ClusterNode
import org.apache.hoya.api.RoleKeys
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hoya.testtools.HBaseTestUtils
import static org.apache.hoya.yarn.Arguments.*
import static org.apache.hoya.testtools.HoyaTestUtils.*
import static org.apache.hoya.HoyaXMLConfKeysForTesting.*
import static org.apache.hoya.providers.hbase.HBaseKeys.*
/**
 * test base for all hbase clusters
 */
@CompileStatic
@Slf4j
public abstract class HBaseMiniClusterTestBase extends YarnMiniClusterTestBase {
  public static final int HBASE_CLUSTER_STARTUP_TIME = HBASE_LAUNCH_WAIT_TIME

  /**
   * The time to sleep before trying to talk to the HBase Master and
   * expect meaningful results.
   */
  public static
  final int HBASE_CLUSTER_STARTUP_TO_LIVE_TIME = HBASE_CLUSTER_STARTUP_TIME

  public static final String HREGION = "HRegion"
  public static final String HMASTER = "HMaster"


  @Override
  public String getTestConfigurationPath() {
    return "src/main/resources/" + HBASE_CONF_RESOURCE;
  }

  @Override
  void setup() {
    super.setup()
    assumeBoolOption(HOYA_CONFIG, KEY_HOYA_TEST_HBASE_ENABLED, true)
    assumeArchiveDefined();
    assumeApplicationHome();
  }

  /**
   * Teardown kills region servers
   */
  @Override
  void teardown() {
    super.teardown();
    killAllRegionServers();
    killAllMasterServers();
  }

  /**
   * Kill all the region servers
   * <code>
   *    jps -l | grep HRegion | awk '{print $1}' | kill -9
   *  </code>
   */
  public void killAllRegionServers() {
    killJavaProcesses(HREGION, SIGKILL);
  }

  /**
   * Kill all master servers
   */
  public void killAllMasterServers() {
    killJavaProcesses(HMASTER, SIGKILL);
  }

  /**
   * Stop all the region servers
   * <code>
   *    jps -l | grep HRegion | awk '{print $1}' | kill -19
   *  </code>
   */
  public void stopAllRegionServers() {
    killJavaProcesses(HREGION, SIGTERM);
  }


  public static void assertHBaseMasterNotStopped(HoyaClient hoyaClient,
                                          String clustername) {
    String[] nodes = hoyaClient.listNodeUUIDsByRole(ROLE_MASTER);
    int masterNodeCount = nodes.length;
    assert masterNodeCount > 0;
    ClusterNode node = hoyaClient.getNode(nodes[0]);
    if (node.state >= ClusterDescription.STATE_STOPPED) {
      //stopped, not what is wanted
      log.error("HBase master has stopped");
      log.error(node.toString());
      fail("HBase master has stopped " + node.diagnostics);
    }
  }
  
  /**
   * Create an (unshared) HConnection talking to the hbase service that
   * Hoya should be running
   * @param hoyaClient hoya client
   * @param clustername the name of the Hoya cluster
   * @return the connection
   */
  public static HConnection createHConnection(Configuration clientConf) {
    return HBaseTestUtils.createHConnection(clientConf)
  }

  /**
   * get a string representation of an HBase cluster status
   * @param status cluster status
   * @return a summary for printing
   */
  public static String hbaseStatusToString(ClusterStatus status) {
    return HBaseTestUtils.hbaseStatusToString(status)
  }

  public static ClusterStatus getHBaseClusterStatus(HoyaClient hoyaClient) {
    return HBaseTestUtils.getHBaseClusterStatus(hoyaClient)
  }

  /**
   * Create an HBase config to work with
   * @param hoyaClient hoya client
   * @param clustername cluster
   * @return an hbase config extended with the custom properties from the
   * cluster, including the binding to the HBase cluster
   */
  public static Configuration createHBaseConfiguration(HoyaClient hoyaClient) {
    return HBaseTestUtils.createHBaseConfiguration(hoyaClient)
  }

  public static ClusterStatus basicHBaseClusterStartupSequence(HoyaClient hoyaClient) {
    return HBaseTestUtils.basicHBaseClusterStartupSequence(hoyaClient,
                                   HBASE_CLUSTER_STARTUP_TIME,
                                   HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
  }

  /**
   * Spin waiting for the RS count to match expected
   * @param hoyaClient client
   * @param clustername cluster name
   * @param regionServerCount RS count
   * @param timeout timeout
   */
  public static ClusterStatus waitForHBaseRegionServerCount(HoyaClient hoyaClient,
                                                     String clustername,
                                                     int regionServerCount,
                                                     int timeout) {

    return HBaseTestUtils.waitForHBaseRegionServerCount(hoyaClient,
                                                        clustername,
                                                        regionServerCount,
                                                        timeout)
  }

  public boolean flexHBaseClusterTestRun(
      String clustername,
      int masters,
      int masterFlexTarget,
      int workers,
      int flexTarget,
      boolean testHBaseAfter) {
    createMiniCluster(clustername, getConfiguration(),
                      1,
                      true);
    //now launch the cluster
    HoyaClient hoyaClient = null;
    ServiceLauncher<HoyaClient> launcher = createHoyaCluster(clustername,
                         [
                             (ROLE_MASTER): masters,
                             (ROLE_WORKER): workers,
                         ],
                         [
                             ARG_ROLEOPT ,
                             ROLE_MASTER ,
                             RoleKeys.YARN_MEMORY ,
                             YRAM,
                             ARG_ROLEOPT ,
                             ROLE_WORKER ,
                             RoleKeys.YARN_MEMORY ,
                             YRAM
                         ],
                         true,
                         true,
                         [:]);
    hoyaClient = launcher.service;
    try {
      basicHBaseClusterStartupSequence(hoyaClient);

      describe("Waiting for initial worker count of $workers");

      //verify the #of roles is as expected
      //get the hbase status
      waitForHoyaWorkerCount(hoyaClient, workers, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME);
      waitForHoyaMasterCount(hoyaClient, masters, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME);

      log.info("Hoya worker count at $workers, waiting for region servers to match");
      waitForHBaseRegionServerCount(hoyaClient, clustername, workers, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME);

      //now flex
      describe("Flexing  masters:$masters -> $masterFlexTarget ; workers $workers -> $flexTarget");
      boolean flexed;
      flexed = 0 == hoyaClient.flex(clustername,
                                    [
                                        (ROLE_WORKER): flexTarget,
                                        (ROLE_MASTER): masterFlexTarget
                                    ],
                                    true);
      waitForHoyaWorkerCount(hoyaClient, flexTarget, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME);
      waitForHoyaMasterCount(hoyaClient, masterFlexTarget,
                             HBASE_CLUSTER_STARTUP_TO_LIVE_TIME);

      if (testHBaseAfter) {
        waitForHBaseRegionServerCount(hoyaClient, clustername, flexTarget,
                                      HBASE_CLUSTER_STARTUP_TO_LIVE_TIME);
      }
      return flexed;
    } finally {
      maybeStopCluster(hoyaClient, null, "end of flex test run");
    }

  }

  /**
   * Spin waiting for the Hoya worker count to match expected
   * @param hoyaClient client
   * @param desiredCount RS count
   * @param timeout timeout
   */
  public static ClusterDescription waitForHoyaWorkerCount(HoyaClient hoyaClient,
                                                   int desiredCount,
                                                   int timeout) {
    return waitForRoleCount(hoyaClient, ROLE_WORKER, desiredCount, timeout)
  }
  
  public static ClusterDescription waitForHoyaMasterCount(HoyaClient hoyaClient,
                                                   int desiredCount,
                                                   int timeout) {
    return waitForRoleCount(hoyaClient, ROLE_MASTER, desiredCount, timeout)
  }


  /**
   * attempt to talk to the hbase master; expect a failure
   * @param clientConf client config
   */
  public void assertNoHBaseMaster(
      HoyaClient hoyaClient, Configuration clientConf) {
    HBaseTestUtils.assertNoHBaseMaster(hoyaClient, clientConf)
  }
  
  /**
   * attempt to talk to the hbase master; expect success
   * @param clientConf client config
   */
  public void assertHBaseMasterFound(Configuration clientConf) {
    HBaseTestUtils.assertHBaseMasterFound(clientConf)
  }

}
