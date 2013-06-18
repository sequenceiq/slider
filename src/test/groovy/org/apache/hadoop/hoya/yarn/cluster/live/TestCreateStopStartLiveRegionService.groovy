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

package org.apache.hadoop.hoya.yarn.cluster.live

import groovy.util.logging.Commons
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hbase.ServerName
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.yarn.ZKIntegration
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@Commons
class TestCreateStopStartLiveRegionService extends YarnMiniClusterTestBase {


  @Test
  public void testLiveRegionServiceTwoNodes() throws Throwable {
    String clustername = "TestLiveRegionService"
    int regionServerCount = 2
    createMiniCluster(clustername, new YarnConfiguration(), regionServerCount+1, true)
    ServiceLauncher launcher = createHoyaCluster(clustername, regionServerCount, [], true, true)
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    ClusterDescription status = hoyaClient.getClusterStatus(clustername)
    log.info("${status.toJsonString()}")

    ClusterStatus clustat = basicHBaseClusterStartupSequence(hoyaClient, clustername)

    status = waitForRegionServerCount(hoyaClient, clustername, regionServerCount, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    clustat = getHBaseClusterStatus(hoyaClient, clustername)

    describe("Cluster status")
    log.info(prettyPrint(status.toJsonString()))
    
    Collection<ServerName> servers = clustat.servers
    if (servers.size() != regionServerCount) {
      log.warn("Server size is not $regionServerCount in " + statusToString(clustat))
    }
    

    clusterActionStop(hoyaClient, clustername)
    
    //now let's start the cluster up again
    ServiceLauncher launcher2 = startHoyaCluster(clustername, [], true);
    HoyaClient newCluster = launcher.getService() as HoyaClient
    newCluster.getClusterStatus(clustername);

    status = waitForRegionServerCount(newCluster, clustername,
                                      regionServerCount,
                                      HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    clustat = getHBaseClusterStatus(newCluster, clustername)

  }




}
