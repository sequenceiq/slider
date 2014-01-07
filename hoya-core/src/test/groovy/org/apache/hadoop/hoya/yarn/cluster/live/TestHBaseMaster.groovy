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
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.HoyaXmlConfKeys
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.api.RoleKeys;
import org.apache.hadoop.hoya.providers.hbase.HBaseKeys
import org.apache.hadoop.hoya.tools.ZKIntegration
import org.apache.hadoop.hoya.yarn.Arguments
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.service.launcher.ServiceLaunchException
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * Create a master against the File:// fs
 */
@CompileStatic
@Slf4j
class TestHBaseMaster extends HBaseMiniClusterTestBase {

  @Test
  public void testHBaseMaster() throws Throwable {
    String clustername = "test_hbase_master"
    createMiniCluster(clustername, createConfiguration(), 1, true)
    //make sure that ZK is up and running at the binding string
    ZKIntegration zki = createZKIntegrationInstance(ZKBinding, clustername, false, false, 5000)
    //now launch the cluster with 1 region server
    int regionServerCount = 1
    ServiceLauncher launcher = createHBaseCluster(clustername, regionServerCount,
      [
        Arguments.ARG_ROLEOPT, HBaseKeys.ROLE_MASTER, RoleKeys.JVM_HEAP, "1G",
        Arguments.ARG_DEFINE, HoyaXmlConfKeys.KEY_HOYA_YARN_QUEUE+"=default"
      ], 
      true,
      true) 
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);
    ClusterDescription status = hoyaClient.getClusterDescription(clustername)
    assert ZKHosts == status.zkHosts
    assert ZKPort == status.zkPort
    
    //dumpFullHBaseConf(hoyaClient, clustername)

    basicHBaseClusterStartupSequence(hoyaClient)
    
    //verify the #of region servers is as expected
    dumpClusterStatus(hoyaClient, "post-hbase-boot status")

    //get the hbase status
    waitForHBaseRegionServerCount(hoyaClient, clustername, 1, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    waitForHoyaWorkerCount(hoyaClient, 1, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    waitForRoleCount(hoyaClient, HBaseKeys.ROLE_MASTER, 1,
                     HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
  }

  @Test
  public void testHBaseMasterWithBadHeap() throws Throwable {
    String clustername = "test_hbase_master_with_bad_heap"
    createMiniCluster(clustername, createConfiguration(), 1, true)

    describe "verify that bad Java heap options are picked up"
    //now launch the cluster with 1 region server
    int regionServerCount = 1
    try {
      ServiceLauncher launcher = createHBaseCluster(clustername, regionServerCount,
        [Arguments.ARG_ROLEOPT, HBaseKeys.ROLE_MASTER, RoleKeys.JVM_HEAP, "invalid"], true, true) 
      HoyaClient hoyaClient = (HoyaClient) launcher.service
      addToTeardown(hoyaClient);
      waitForClusterLive(hoyaClient)
      fail("expected a failure, got a live cluster")
    } catch (ServiceLaunchException e) {
      assertExceptionDetails(e, HoyaExitCodes.EXIT_CLUSTER_FAILED)
    }
  }
}
