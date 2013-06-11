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





package org.apache.hadoop.hoya.yarn.cluster.actions

import groovy.util.logging.Commons
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.tools.Duration
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.hoya.yarn.client.ClientArgs
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@Commons
class TestHBaseVersionCommand extends YarnMiniClusterTestBase {

  @Test
  public void testHBaseVersionCommand() throws Throwable {
    String clustername = "TestHBaseVersionCommand"
    createMiniCluster(clustername, new YarnConfiguration(), 1, true)
    log.info("RM address = ${RMAddr}")
    ServiceLauncher launcher = launchHoyaClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            ClientArgs.ACTION_CREATE, clustername,
            CommonArgs.ARG_MIN, "1",
            CommonArgs.ARG_MAX, "1",
            ClientArgs.ARG_MANAGER, RMAddr,
            CommonArgs.ARG_USER, USERNAME,
            CommonArgs.ARG_HBASE_HOME, HBaseHome,
            CommonArgs.ARG_ZOOKEEPER, microZKCluster.zkBindingString,
            CommonArgs.ARG_HBASE_ZKPATH, "/test/TestHBaseVersionCommand",
            ClientArgs.ARG_WAIT, WAIT_TIME_ARG,
            CommonArgs.ARG_X_TEST,
            CommonArgs.ARG_X_HBASE_COMMAND, "version"
        ]
    )
    assert launcher.serviceExitCode == 0
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    hoyaClient.monitorAppToRunning(new Duration(CLUSTER_GO_LIVE_TIME))
    ClusterDescription status = hoyaClient.getClusterStatus(clustername)
    log.info("Status $status")
    waitForAppToFinish(hoyaClient)
  }

}
