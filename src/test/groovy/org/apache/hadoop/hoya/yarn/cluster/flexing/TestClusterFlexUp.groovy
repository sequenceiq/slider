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

package org.apache.hadoop.hoya.yarn.cluster.flexing

import groovy.util.logging.Commons
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * Create a master against the File:// fs
 */
@Commons
class TestClusterFlexUp extends YarnMiniClusterTestBase {

  @Test
  public void testClusterFlexUp() throws Throwable {
    String clustername = "TestClusterFlexUp"
    createMiniCluster(clustername, createConfiguration(), 1, true)
    //now launch the cluster
    int workers = 1
    ServiceLauncher launcher = createHoyaCluster(clustername, workers, [], true, true)
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    ClusterDescription status = hoyaClient.getClusterStatus(clustername)
    basicHBaseClusterStartupSequence(hoyaClient, clustername)

    describe("Waiting for initial worker count of $workers")

    //verify the #of region servers is as expected
    //get the hbase status
    waitForRegionServerCount(hoyaClient, clustername, workers, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    waitForHBaseWorkerCount(hoyaClient, clustername, workers, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)

    //start to add some more workers
    for (increment in [1]) {
      workers += increment
      describe("Adding $increment workers to a total of $workers")
      assert hoyaClient.actionFlex(clustername, workers, 0, true)
      waitForHBaseWorkerCount(hoyaClient, clustername, workers, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
      waitForRegionServerCount(hoyaClient, clustername, workers, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    }

    clusterActionStop(hoyaClient, clustername)

  }

}
