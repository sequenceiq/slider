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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hoya.HoyaXMLConfKeysForTesting
import org.apache.hoya.HoyaXmlConfKeys
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.api.StatusKeys
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.params.ActionAMSuicideArgs
import org.apache.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.junit.Test

/**
 * test create a live region service
 */
@CompileStatic
@Slf4j
class TestKilledAM extends HBaseMiniClusterTestBase {

  @Test
  public void testKilledAM() throws Throwable {
    String clustername = "test_killed_am"
    int regionServerCount = 1


    def conf = configuration
    // patch the configuration for AM restart
    conf.setInt(HoyaXmlConfKeys.KEY_HOYA_RESTART_LIMIT, 3)

    conf.setClass(YarnConfiguration.RM_SCHEDULER,
                  FifoScheduler, ResourceScheduler);
    createMiniCluster(clustername, conf, 1, 1, 1, true, true)
    describe(" Kill the AM, expect cluster to die");

    //now launch the cluster
    ServiceLauncher<HoyaClient> launcher = createHBaseCluster(
        clustername,
        regionServerCount,
        [],
        true,
        true)
    HoyaClient hoyaClient = launcher.service
    addToTeardown(hoyaClient);
    ClusterDescription status = hoyaClient.getClusterDescription(clustername)

    ClusterStatus clustat = basicHBaseClusterStartupSequence(hoyaClient)


    status = waitForHoyaWorkerCount(
        hoyaClient,
        regionServerCount,
        HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    //get the hbase status
    ClusterStatus hbaseStat = waitForHBaseRegionServerCount(
        hoyaClient,
        clustername,
        regionServerCount,
        HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)

    log.info("Initial cluster status : ${hbaseStatusToString(hbaseStat)}");

    String hbaseMasterContainer = status.instances[HBaseKeys.ROLE_MASTER][0]

    Configuration clientConf = createHBaseConfiguration(hoyaClient)
    clientConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    HConnection hbaseConnection
    hbaseConnection = createHConnection(clientConf)



    describe("running processes")
    lsJavaProcesses()
    describe("killing AM")

    ActionAMSuicideArgs args = new ActionAMSuicideArgs()
    args.message = "test AM"
    args.waittime = 1000
    args.exitcode = 1
    hoyaClient.actionAmSuicide(clustername, args)

    killAllRegionServers();
    waitWhileClusterLive(hoyaClient, 30000);
    // give yarn some time to notice
    sleep(10000)

    // policy here depends on YARN behavior
    if (!HoyaXMLConfKeysForTesting.YARN_AM_SUPPORTS_RESTART) {
      // kill hbase masters for OS/X tests to pass
      killAllMasterServers();
      // expect hbase connection to have failed
      assertNoHBaseMaster(hoyaClient, clientConf)
    }
    // await cluster startup
    ApplicationReport report = hoyaClient.applicationReport
    assert report.yarnApplicationState != YarnApplicationState.FAILED;

    status = waitForHoyaWorkerCount(
        hoyaClient,
        regionServerCount,
        HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)

    if (HoyaXMLConfKeysForTesting.YARN_AM_SUPPORTS_RESTART) {

      dumpClusterDescription("post-restart status", status)
      // verify the AM restart container count was set
      String restarted = status.getInfo(
          StatusKeys.INFO_CONTAINERS_AM_RESTART)
      assert restarted != null;
      //and that the count == 1 master (the region servers were killed)
      assert Integer.parseInt(restarted) == 1

      // now verify the master container is as before (with strict checks for incomplete data)
  
      assert null != status.instances[HBaseKeys.ROLE_MASTER];
      assert 1 == status.instances[HBaseKeys.ROLE_MASTER].size();
      assert hbaseMasterContainer == status.instances[HBaseKeys.ROLE_MASTER][0]
    }

    waitForHBaseRegionServerCount(
        hoyaClient,
        clustername,
        regionServerCount,
        HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)

  }

}
