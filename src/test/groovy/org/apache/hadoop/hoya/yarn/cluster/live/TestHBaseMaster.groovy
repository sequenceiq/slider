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
import groovy.util.logging.Commons
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.yarn.ZKIntegration
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * Create a master against the File:// fs
 */
@CompileStatic
@Commons
class TestHBaseMaster extends YarnMiniClusterTestBase {

  @Test
  public void testHBaseMaster() throws Throwable {
    String clustername = "TestHBaseMaster"
    createMiniCluster(clustername, createConfiguration(), 1, true)
    //make sure that ZK is up and running at the binding string
    ZKIntegration zki = createZKIntegrationInstance(ZKBinding, clustername, false, false, 5000)
    log.info("ZK up at $zki");
    //now launch the cluster
    int regionServerCount = 1
    ServiceLauncher launcher = createHoyaCluster(clustername, regionServerCount, [], true, true) 
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    ClusterDescription status = hoyaClient.getClusterStatus(clustername)
    assert ZKHosts == status.zkHosts
    assert ZKPort == status.zkPort
    
    //dumpFullHBaseConf(hoyaClient, clustername)

    basicHBaseClusterStartupSequence(hoyaClient, clustername)
    
    //verify the #of region servers is as expected
    status = hoyaClient.getClusterStatus(clustername)
    log("post-hbase-boot status", status)
    //get the hbase status
    waitForHoyaWorkerCount(hoyaClient, clustername, 1, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    waitForRegionServerCount(hoyaClient, clustername, 1, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)


    clusterActionFreeze(hoyaClient, clustername)

  }

}
