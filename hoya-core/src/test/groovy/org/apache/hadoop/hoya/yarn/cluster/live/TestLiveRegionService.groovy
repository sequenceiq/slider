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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.tools.ZKIntegration
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * test create a live region service
 */
@CompileStatic
@Slf4j
class TestLiveRegionService extends HBaseMiniClusterTestBase {

  @Test
  public void testLiveRegionServiceSingleNode() throws Throwable {
    String clustername = "TestLiveRegionService"
    int regionServerCount = 1
    createMiniCluster(clustername, createConfiguration(), 1, 1, 1, true, true)
    describe(" Create a single region service cluster");

    //make sure that ZK is up and running at the binding string
    ZKIntegration zki = createZKIntegrationInstance(ZKBinding, clustername, false, false, 5000)
    log.info("ZK up at $zki");
    //now launch the cluster
    ServiceLauncher launcher = createHBaseCluster(clustername, regionServerCount, [], true, true)
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);
    ClusterDescription status = hoyaClient.getClusterStatus(clustername)
    log.info("${status.toJsonString()}")
    assert ZKHosts == status.zkHosts
    assert ZKPort == status.zkPort

    dumpFullHBaseConf(hoyaClient)

    ClusterStatus clustat = basicHBaseClusterStartupSequence(hoyaClient)


    status = waitForHoyaWorkerCount(hoyaClient, regionServerCount, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    describe("Cluster status")
    log.info(prettyPrint(status.toJsonString()))
    //get the hbase status
    waitForHBaseRegionServerCount(hoyaClient, clustername, regionServerCount, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)

  }

}
