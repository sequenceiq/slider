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

package org.apache.hadoop.hoya.yarn.cluster.failures

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.RetriesExhaustedException
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.providers.hbase.HBaseKeys
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * test create a live region service
 */
@CompileStatic
@Slf4j
class TestKilledAM extends HBaseMiniClusterTestBase {

  @Test
  public void testKilledAM() throws Throwable {
    String clustername = "TestKilledAM"
    int regionServerCount = 1
    createMiniCluster(clustername, createConfiguration(), 1, 1, 1, true, true)
    describe(" Kill the AM, expect cluster to die");

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
    Configuration clientConf = createHBaseConfiguration(hoyaClient)
    clientConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,1);
    HConnection hbaseConnection
    hbaseConnection = createHConnection(clientConf)



    describe("running processes")
    lsJavaProcesses()
    describe("killing services")
    killServiceLaunchers(SIGTERM);
    killAllMasterServers();
    waitWhileClusterExists(hoyaClient, 30000);
    //give yarn some time to notice
    sleep(2000)
    ApplicationReport report = hoyaClient.applicationReport
    assert report.yarnApplicationState == YarnApplicationState.FAILED;
    describe("final listing")
    lsJavaProcesses();
    //expect hbase connection to have failed

    assertNoHBaseMaster(clientConf)

  }

}
