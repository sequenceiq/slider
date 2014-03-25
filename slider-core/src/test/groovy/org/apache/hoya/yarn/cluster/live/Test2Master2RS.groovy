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

package org.apache.hoya.yarn.cluster.live

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.testtools.HBaseTestUtils
import org.apache.hoya.tools.Duration
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.junit.Test

/**
 * test create a live region service
 */
@CompileStatic
@Slf4j

class Test2Master2RS extends HBaseMiniClusterTestBase {

  @Test
  public void test2Master2RS() throws Throwable {

    String clustername = "test2master2rs"
    int regionServerCount = 2
    createMiniCluster(clustername, getConfiguration(), 1, 1, 1, true, false)

    describe(" Create a two master, two region service cluster");
    //now launch the cluster
    int masterCount = 2

    ServiceLauncher launcher = createHBaseCluster(
        clustername,
        masterCount,
        regionServerCount,
        [],
        true,
        true)
    
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);
    ClusterDescription status = hoyaClient.getClusterDescription(clustername)
    log.info("${status.toJsonString()}")
    assert ZKHosts == status.zkHosts
    assert ZKPort == status.zkPort

    ClusterStatus clustat = basicHBaseClusterStartupSequence(hoyaClient)



    status = waitForHoyaWorkerCount(
        hoyaClient,
        regionServerCount,
        HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    //get the hbase status

    Duration duration = new Duration(HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    duration.start()

    Configuration clientConf = HBaseTestUtils.createHBaseConfiguration(
        hoyaClient)


    while (!duration.limitExceeded && clustat.backupMastersSize != 1) {
      clustat = HBaseTestUtils.getHBaseClusterStatus(clientConf);
      Thread.sleep(1000);
    }
    if (duration.limitExceeded) {
      HBaseTestUtils.describe(
          "Cluster region server count of ${regionServerCount} not met:");
      log.info(HBaseTestUtils.hbaseStatusToString(clustat));
      fail("Backup master count not reached");
    }

  }
}

