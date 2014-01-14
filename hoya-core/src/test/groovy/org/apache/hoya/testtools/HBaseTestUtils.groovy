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

package org.apache.hoya.testtools

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
import org.apache.hoya.HoyaKeys
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.tools.ConfigHelper
import org.apache.hoya.tools.Duration
import org.apache.hoya.yarn.client.HoyaClient

/**
 * Static HBase test utils
 */
@Slf4j
@CompileStatic
class HBaseTestUtils extends HoyaTestUtils {

  /**
   * Create an (unshared) HConnection talking to the hbase service that
   * Hoya should be running
   * @param hoyaClient hoya client
   * @param clustername the name of the Hoya cluster
   * @return the connection
   */
  public static HConnection createHConnection(Configuration clientConf) {
    HConnection hbaseConnection = HConnectionManager.createConnection(
        clientConf);
    return hbaseConnection
  }

  /**
   * get a string representation of an HBase cluster status
   * @param status cluster status
   * @return a summary for printing
   */
  public static String hbaseStatusToString(ClusterStatus status) {
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

  public static ClusterStatus getHBaseClusterStatus(HoyaClient hoyaClient) {
    Configuration clientConf = createHBaseConfiguration(hoyaClient)
    return getHBaseClusterStatus(clientConf)
  }

  public static ClusterStatus getHBaseClusterStatus(Configuration clientConf) {
    try {
      HConnection hbaseConnection1 = createHConnection(clientConf)
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
  public static Configuration createHBaseConfiguration(HoyaClient hoyaClient) {
    Configuration siteConf = fetchClientSiteConfig(hoyaClient);
    Configuration conf = HBaseConfiguration.create(siteConf);
    return conf
  }

  /**
   * Ask the AM for the site configuration -then dump it
   * @param hoyaClient
   * @param clustername
   */
  public static void dumpHBaseClientConf(HoyaClient hoyaClient) {
    Configuration conf = fetchClientSiteConfig(hoyaClient);
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
  public static void dumpFullHBaseConf(HoyaClient hoyaClient) {
    Configuration conf = createHBaseConfiguration(hoyaClient);
    describe("HBase site configuration from AM");
    ConfigHelper.dumpConf(conf);
  }


  public static ClusterStatus basicHBaseClusterStartupSequence(
      HoyaClient hoyaClient, int startupTime, int startupToLiveTime ) {
    int state = hoyaClient.waitForRoleInstanceLive(HoyaKeys.ROLE_HOYA_AM,
                                                   startupTime);
    assert state == ClusterDescription.STATE_LIVE;
    state = hoyaClient.waitForRoleInstanceLive(HBaseKeys.ROLE_MASTER,
                                               startupTime);
    assert state == ClusterDescription.STATE_LIVE;
    //sleep for a bit to give things a chance to go live
    assert spinForClusterStartup(
        hoyaClient,
        startupToLiveTime);

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
  public static ClusterStatus waitForHBaseRegionServerCount(
      HoyaClient hoyaClient,
      String clustername,
      int regionServerCount,
      int timeout) {
    Duration duration = new Duration(timeout);
    duration.start();
    ClusterStatus clustat = null;
    Configuration clientConf = createHBaseConfiguration(hoyaClient)
    while (true) {
      clustat = getHBaseClusterStatus(clientConf);
      int workerCount = clustat.servers.size();
      if (workerCount == regionServerCount) {
        break;
      }
      if (duration.limitExceeded) {
        describe("Cluster region server count of $regionServerCount not met:");
        log.info(hbaseStatusToString(clustat));
        ClusterDescription status = hoyaClient.getClusterDescription(
            clustername);
        fail("Expected $regionServerCount YARN region servers," +
             " but  after $timeout millis saw $workerCount in ${hbaseStatusToString(clustat)}" +
             " \n ${prettyPrint(status.toJsonString())}");
      }
      log.info(
          "Waiting for $regionServerCount region servers -got $workerCount");
      Thread.sleep(1000);
    }
    return clustat;
  }

  /**
   * Probe to test for the HBasemaster being found
   * @param clientConf client configuration
   * @return true if the master was found in the hbase cluster status
   */
  public static boolean isHBaseMasterFound(Configuration clientConf) {
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

  /**
   * attempt to talk to the hbase master; expect a failure
   * @param clientConf client config
   */
  public static void assertNoHBaseMaster(Configuration clientConf) {
    boolean masterFound = isHBaseMasterFound(clientConf)
    if (masterFound) {
      fail("HBase master running")
    }
  }

  /**
   * attempt to talk to the hbase master; expect success
   * @param clientConf client config
   */
  public static void assertHBaseMasterFound(Configuration clientConf) {
    boolean masterFound = isHBaseMasterFound(clientConf)
    if (!masterFound) {
      fail("HBase master not running")
    }
  }
  
}
