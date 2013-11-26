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

package org.apache.hadoop.hoya.yarn.providers.hbase

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.ServerName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.RetriesExhaustedException
import org.apache.hadoop.hoya.HoyaKeys
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.api.ClusterNode
import org.apache.hadoop.hoya.api.RoleKeys
import org.apache.hadoop.hoya.providers.hbase.HBaseKeys
import org.apache.hadoop.hoya.tools.ConfigHelper
import org.apache.hadoop.hoya.tools.Duration
import org.apache.hadoop.hoya.yarn.Arguments
import org.apache.hadoop.hoya.yarn.KeysForTests
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Assume

/**
 * test base for all hbase clusters
 */
@CompileStatic
@Slf4j
public class HBaseMiniClusterTestBase extends YarnMiniClusterTestBase {


  @Override
  public String getTestConfigurationPath() {
    return "src/main/resources/" + HBaseKeys.HBASE_CONF_RESOURCE;
  }

  @Override
  void setup() {
    super.setup()
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
    killJavaProcesses(HREGION, SIGSTOP);
  }


  public void assertHBaseMasterNotStopped(HoyaClient hoyaClient,
                                          String clustername) {
    String[] nodes = hoyaClient.listNodeUUIDsByRole(HBaseKeys.ROLE_MASTER);
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
   * Fetch the current hbase site config from the Hoya AM, from the 
   * <code>hBaseClientProperties</code> field of the ClusterDescription
   * @param hoyaClient client
   * @param clustername name of the cluster
   * @return the site config
   */
  public Configuration fetchHBaseClientSiteConfig(HoyaClient hoyaClient) {
    ClusterDescription status = hoyaClient.clusterDescription;
    Configuration siteConf = new Configuration(false)
    status.clientProperties.each { String key, String val ->
      siteConf.set(key, val, "hoya cluster");
    }
    return siteConf;
  }

  /**
   * Create an (unshared) HConnection talking to the hbase service that
   * Hoya should be running
   * @param hoyaClient hoya client
   * @param clustername the name of the Hoya cluster
   * @return the connection
   */
  
  public HConnection createHConnection(Configuration clientConf) {
    HConnection hbaseConnection = HConnectionManager.createConnection(clientConf);
    return hbaseConnection
  }

  /**
   * get a string representation of an HBase cluster status
   * @param status cluster status
   * @return a summary for printing
   */
  String hbaseStatusToString(ClusterStatus status) {
    StringBuilder builder = new StringBuilder();
    builder << "Cluster " << status.clusterId;
    builder << " @ " << status.master << " version " << status.HBaseVersion;
    builder << "\nlive [\n"
    if (!status.servers.empty) {
      status.servers.each() { ServerName name ->
        builder << " Server " << name << " :" << status.getLoad(name) << "\n"
      }
    } else {
    }
    builder << "]\n"
    if (status.deadServers > 0) {
      builder << "\n dead servers=${status.deadServers}"
    }
    return builder.toString()
  }

  public ClusterStatus getHBaseClusterStatus(HoyaClient hoyaClient) {
    try {
      Configuration clientConf1 = createHBaseConfiguration(hoyaClient)
      HConnection hbaseConnection1 = createHConnection(clientConf1)
      HConnection hbaseConnection = hbaseConnection1;
      HBaseAdmin hBaseAdmin = new HBaseAdmin(hbaseConnection);
      ClusterStatus hBaseClusterStatus = hBaseAdmin.clusterStatus;
      return hBaseClusterStatus;
    } catch (NoSuchMethodError e) {
      throw new Exception("Using an incompatible version of HBase!", e);
    }

  }

  /**
   * Create an HBase config to work with
   * @param hoyaClient hoya client
   * @param clustername cluster
   * @return an hbase config extended with the custom properties from the
   * cluster, including the binding to the HBase cluster
   */
  public Configuration createHBaseConfiguration(HoyaClient hoyaClient) {
    Configuration siteConf = fetchHBaseClientSiteConfig(hoyaClient);
    Configuration conf = HBaseConfiguration.create(siteConf);
/*
    
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
    conf.setInt("zookeeper.recovery.retry", 0)

*/
    return conf
  }
  /**
   * Ask the AM for the site configuration -then dump it
   * @param hoyaClient
   * @param clustername
   */
  public void dumpHBaseClientConf(HoyaClient hoyaClient) {
    Configuration conf = fetchHBaseClientSiteConfig(hoyaClient);
    describe("AM-generated site configuration");
    ConfigHelper.dumpConf(conf);
  }

  /**
   * Create a full HBase configuration by merging the AM data with
   * the rest of the local settings. This is the config that would
   * be used by any clients
   * @param hoyaClient hoya client
   * @param clustername name of the cluster
   */
  public void dumpFullHBaseConf(HoyaClient hoyaClient) {
    Configuration conf = createHBaseConfiguration(hoyaClient);
    describe("HBase site configuration from AM");
    ConfigHelper.dumpConf(conf);
  }


  public ClusterStatus basicHBaseClusterStartupSequence(HoyaClient hoyaClient) {
    int state = hoyaClient.waitForRoleInstanceLive(HoyaKeys.ROLE_HOYA_AM,
                                                        HBASE_CLUSTER_STARTUP_TIME);
    assert state == ClusterDescription.STATE_LIVE;
    state = hoyaClient.waitForRoleInstanceLive(HBaseKeys.ROLE_MASTER,
                                                        HBASE_CLUSTER_STARTUP_TIME);
    assert state == ClusterDescription.STATE_LIVE;
    //sleep for a bit to give things a chance to go live
    assert spinForClusterStartup(hoyaClient, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME);

    //grab the conf from the status and verify the ZK binding matches

    ClusterStatus clustat = getHBaseClusterStatus(hoyaClient);
    describe("HBASE CLUSTER STATUS \n " + hbaseStatusToString(clustat));
    return clustat;
  }

  /**
   * Spin waiting for the RS count to match expected
   * @param hoyaClient client
   * @param clustername cluster name
   * @param regionServerCount RS count
   * @param timeout timeout
   */
  public ClusterStatus waitForHBaseRegionServerCount(HoyaClient hoyaClient,
                                                     String clustername,
                                                     int regionServerCount,
                                                     int timeout) {
    Duration duration = new Duration(timeout);
    duration.start();
    ClusterStatus clustat = null;
    while (true) {
      clustat = getHBaseClusterStatus(hoyaClient);
      int workerCount = clustat.servers.size();
      if (workerCount == regionServerCount) {
        break;
      }
      if (duration.limitExceeded) {
        describe("Cluster region server count of $regionServerCount not met:");
        log.info(hbaseStatusToString(clustat));
        ClusterDescription status = hoyaClient.getClusterDescription(clustername);
        fail("Expected $regionServerCount YARN region servers," +
             " but  after $timeout millis saw $workerCount in ${hbaseStatusToString(clustat)}" +
             " \n ${prettyPrint(status.toJsonString())}");
      }
      log.info("Waiting for $regionServerCount region servers -got $workerCount");
      Thread.sleep(1000);
    }
    return clustat;
  }

  public boolean flexHBaseClusterTestRun(
      String clustername,
      int masters,
      int masterFlexTarget,
      int workers,
      int flexTarget,
      boolean persist,
      boolean testHBaseAfter) {
    createMiniCluster(clustername, createConfiguration(),
                      1,
                      true);
    //now launch the cluster
    HoyaClient hoyaClient = null;
    ServiceLauncher launcher = createHoyaCluster(clustername,
                         [
                             (HBaseKeys.ROLE_MASTER): masters,
                             (HBaseKeys.ROLE_WORKER): workers,
                         ],
                         [
                             Arguments.ARG_ROLEOPT ,
                             HBaseKeys.ROLE_MASTER ,
                             RoleKeys.YARN_MEMORY ,
                             YRAM,
                             Arguments.ARG_ROLEOPT ,
                             HBaseKeys.ROLE_WORKER ,
                             RoleKeys.YARN_MEMORY ,
                             YRAM
                             
                         ],
                         true,
                         true, [:]);
    hoyaClient = (HoyaClient) launcher.service;
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
                                        (HBaseKeys.ROLE_WORKER): flexTarget,
                                        (HBaseKeys.ROLE_MASTER): masterFlexTarget
                                    ],
                                    persist);
      waitForHoyaWorkerCount(hoyaClient, flexTarget, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME);
      waitForHoyaMasterCount(hoyaClient, masterFlexTarget,
                             HBASE_CLUSTER_STARTUP_TO_LIVE_TIME);

      if (testHBaseAfter) {
        waitForHBaseRegionServerCount(hoyaClient, clustername, flexTarget, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME);
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
  public ClusterDescription waitForHoyaWorkerCount(HoyaClient hoyaClient,
                                                   int desiredCount,
                                                   int timeout) {
    return waitForRoleCount(hoyaClient, HBaseKeys.ROLE_WORKER, desiredCount, timeout)
  }
  public ClusterDescription waitForHoyaMasterCount(HoyaClient hoyaClient,
                                                   int desiredCount,
                                                   int timeout) {
    return waitForRoleCount(hoyaClient, HBaseKeys.ROLE_MASTER, desiredCount, timeout)
  }


  /**
   * attempt to talk to the hbase master; expect a failure
   * @param clientConf client config
   */
  public void assertNoHBaseMaster(Configuration clientConf) {
    boolean masterFound = isHBaseMasterFound(clientConf)
    if (masterFound) {
      fail("HBase master running")
    }
  }
  /**
   * attempt to talk to the hbase master; expect success
   * @param clientConf client config
   */
  public void assertHBaseMaster(Configuration clientConf) {
    boolean masterFound = isHBaseMasterFound(clientConf)
    if (masterFound) {
      fail("HBase master running")
    }
  }

  public boolean isHBaseMasterFound(Configuration clientConf) {
    HConnection hbaseConnection
    hbaseConnection = createHConnection(clientConf)
    HBaseAdmin hBaseAdmin = new HBaseAdmin(hbaseConnection);
    boolean masterFound
    try {
      ClusterStatus hBaseClusterStatus = hBaseAdmin.getClusterStatus();
      masterFound = true;
    } catch (RetriesExhaustedException e) {
      masterFound = false;
    }
    return masterFound
  }
}
