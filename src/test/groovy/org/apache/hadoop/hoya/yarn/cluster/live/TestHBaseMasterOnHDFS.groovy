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
import org.apache.hadoop.hoya.tools.Duration
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@Commons
class TestHBaseMasterOnHDFS extends YarnMiniClusterTestBase {

  @Test
  public void testHBaseMasteOnHDFSr() throws Throwable {
    String clustername = "TestHBaseMasterOnHDFS"
    createMiniCluster(clustername, new YarnConfiguration(), 1, 1, 1, true, true)
    log.info("HDFS is at $fsDefaultName")
    assert fsDefaultName.startsWith("hdfs://")
    ServiceLauncher launcher = createHoyaCluster(clustername, 0, [], true) 
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    ClusterDescription status = hoyaClient.getClusterStatus(clustername)
    log.info("Status $status")
    assertHBaseMasterNotStopped(hoyaClient, clustername)
    //stop the cluster
    hoyaClient.stop()
    hoyaClient.monitorAppToCompletion(
        new Duration(CLUSTER_GO_LIVE_TIME))
    int hbaseState = hoyaClient.waitForHBaseMasterLive(clustername, CLUSTER_GO_LIVE_TIME);
    assert hbaseState == ClusterDescription.STATE_LIVE
    log.info(statusToString(getHBaseClusterStatus(hoyaClient, clustername)))

  }


}
