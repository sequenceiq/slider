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

package org.apache.hoya.yarn.cluster.freezethaw

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hoya.HoyaExitCodes
import org.apache.hoya.HoyaKeys
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.api.RoleKeys
import org.apache.hoya.exceptions.HoyaException
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@CompileStatic
@Slf4j
class TestFreezeThawLiveRegionService extends HBaseMiniClusterTestBase {

  @Test
  public void testFreezeThawLiveRegionService() throws Throwable {
    String clustername = "test_freeze_thaw_live_regionservice"
    int regionServerCount = 2
    createMiniCluster(clustername, getConfiguration(), 1, true)
    describe("Create a cluster, freeze it, thaw it and verify that it came back ")
    //use a smaller AM HEAP to include it in the test cycle
    ServiceLauncher launcher = createHBaseCluster(clustername, regionServerCount,
          [
              Arguments.ARG_COMP_OPT, HoyaKeys.COMPONENT_AM, RoleKeys.JVM_HEAP, "96M",
          ],
                                                  true, true)
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);
    ClusterDescription status = hoyaClient.getClusterDescription(clustername)
    log.info("${status.toJsonString()}")

    ClusterStatus clustat = basicHBaseClusterStartupSequence(hoyaClient)

    clustat = waitForHBaseRegionServerCount(hoyaClient, clustername, regionServerCount,
                            HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    describe("Cluster status")
    log.info(hbaseStatusToString(clustat));
    

    //verify you can't start a new cluster with that name
    try {
      ServiceLauncher launcher3 = createHBaseCluster(clustername, regionServerCount, [], false, false)
      HoyaClient cluster3 = launcher3.service as HoyaClient
      fail("expected a failure, got ${cluster3}")
    } catch (HoyaException e) {
      assert e.exitCode == HoyaExitCodes.EXIT_CLUSTER_IN_USE;
    }
    
    
    clusterActionFreeze(hoyaClient, clustername)
    killAllRegionServers();
    //now let's start the cluster up again
    ServiceLauncher launcher2 = thawHoyaCluster(clustername, [], true);
    HoyaClient newCluster = launcher2.service as HoyaClient
    basicHBaseClusterStartupSequence(newCluster)

    //get the hbase status
    waitForHBaseRegionServerCount(newCluster, clustername, regionServerCount,
                            HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    
    // finally, attempt to thaw it while it is running
    //now let's start the cluster up again
    try {
      ServiceLauncher launcher3 = thawHoyaCluster(clustername, [], true);
      HoyaClient cluster3 = launcher3.service as HoyaClient
      fail("expected a failure, got ${cluster3}")
    } catch (HoyaException e) {
      assert e.exitCode == HoyaExitCodes.EXIT_CLUSTER_IN_USE
    }
  }




}
