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
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test


//@CompileStatic
@Commons
class TestTwoLiveClusters extends YarnMiniClusterTestBase {

  /**
   * Create two clusters simultaneously and verify that their lifecycle is
   * independent.
   * @throws Throwable
   */
  @Test
  public void testTwoLiveClusters() throws Throwable {
    createMiniCluster("TestTwoLiveClusters", createConfiguration(), 1, true)
    String clustername1 = "TestTwoLiveClusters-a"
    //now launch the cluster
    int regionServerCount = 1
    ServiceLauncher launcher = createHoyaCluster(clustername1, regionServerCount, [], true, true) 
    HoyaClient hoyaClient = (HoyaClient) launcher.service

    basicHBaseClusterStartupSequence(hoyaClient, clustername1)
    
    //verify the #of region servers is as expected
    ClusterDescription status = hoyaClient.getClusterStatus(clustername1)
    log("post-hbase-boot status", status)
    //get the hbase status
    waitForHoyaWorkerCount(hoyaClient, clustername1, 1, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    waitForRegionServerCount(hoyaClient, clustername1, 1, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)

    //now here comes cluster #2
    String clustername2 = "TestTwoLiveClusters-b"

    launcher = createHoyaCluster(clustername2, regionServerCount,
                                 [
                                 CommonArgs.ARG_HBASE_ZKPATH, "/$clustername2",
                                 ],
                                 true,
                                 true)
    HoyaClient cluster2Client = launcher.service as HoyaClient

    basicHBaseClusterStartupSequence(cluster2Client, clustername2)
    waitForHoyaWorkerCount(cluster2Client, clustername2, 1, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    waitForRegionServerCount(cluster2Client, clustername2, 1, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)

    //and now verify that cluster 1 is still happy
    waitForHoyaWorkerCount(hoyaClient, clustername1, 1, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)


    clusterActionFreeze(cluster2Client, clustername2)
    clusterActionFreeze(hoyaClient, clustername1)

  }

}
