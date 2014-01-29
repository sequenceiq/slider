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

package org.apache.hoya.yarn.cluster.failures

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.SleepJob
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.api.RoleKeys
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.params.ActionKillContainerArgs
import org.apache.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * test create a live region service
 */
@CompileStatic
@Slf4j
class TestFailedRegionService extends HBaseMiniClusterTestBase {

  @Test
  public void testFailedRegionService() throws Throwable {
    testRegionService("test_failed_region_service", true)
  }
  
  @Test
  public void testStoppedRegionService() throws Throwable {
    testRegionService("test_stopped_region_service", false)
  }
  
  private void testRegionService(String testName, boolean toKill) {
    String clustername = testName
    String action = toKill ? "kill" : "stop"
    int regionServerCount = 2
    createMiniCluster(clustername, getConfiguration(), 1, 1, 1, true, true)
    describe("Create a single region service cluster then " + action + " the RS");

    //now launch the cluster
    ServiceLauncher launcher = createHBaseCluster(clustername, regionServerCount, [], true, true)
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);
    ClusterDescription status = hoyaClient.getClusterDescription(clustername)

    ClusterStatus clustat = basicHBaseClusterStartupSequence(hoyaClient)

    status = waitForHoyaWorkerCount(hoyaClient, regionServerCount, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    //get the hbase status
    ClusterStatus hbaseStat = waitForHBaseRegionServerCount(hoyaClient, clustername, regionServerCount, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    
    log.info("Initial cluster status : ${hbaseStatusToString(hbaseStat)}");
    describe("running processes")
    lsJavaProcesses()
    describe("about to " + action + " servers")
    if (toKill) {
      killAllRegionServers()
    } else {
      stopAllRegionServers()
    }

    //sleep a bit
    sleep(toKill ? 15000 : 30000);
    lsJavaProcesses()

    describe("waiting for recovery")

    //and expect a recovery
    status = waitForHoyaWorkerCount(hoyaClient, regionServerCount, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
  
    //now we expect the failure count to be two

    
    hbaseStat = waitForHBaseRegionServerCount(hoyaClient, clustername, regionServerCount, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)

    status = hoyaClient.getClusterDescription()
    assert status.roles[HBaseKeys.ROLE_WORKER][RoleKeys.ROLE_FAILED_INSTANCES] == "2"

    log.info("Updated cluster status : ${hbaseStatusToString(hbaseStat)}");
    
    //now attempt it by container kill command
    def workers = status.instances[HBaseKeys.ROLE_WORKER]
    assert workers.size() == 2
    def worker1 = workers[0]
    ActionKillContainerArgs args = new ActionKillContainerArgs();
    args.id = worker1
    assert 0 == hoyaClient.actionKillContainer(clustername, args)
    sleep(15000)
    waitForHoyaWorkerCount(
        hoyaClient,
        regionServerCount,
        HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    status = hoyaClient.getClusterDescription()
    assert status.roles[HBaseKeys.ROLE_WORKER][RoleKeys.ROLE_FAILED_INSTANCES] == "3"
  }

}
