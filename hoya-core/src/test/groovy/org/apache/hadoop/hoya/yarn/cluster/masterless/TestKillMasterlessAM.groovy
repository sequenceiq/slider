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

package org.apache.hadoop.hoya.yarn.cluster.masterless

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@CompileStatic
@Slf4j

class TestKillMasterlessAM extends HBaseMiniClusterTestBase {


  @Test
  public void testKillMasterlessAM() throws Throwable {
    String clustername = "test_kill_masterless_am"
    createMiniCluster(clustername, createConfiguration(), 1, true)

    describe "kill a masterless AM and verify that it shuts down"

    ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, true)
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);
    describe("listing services")
    lsJavaProcesses();
    describe("killing services")
    killServiceLaunchers(SIGTERM);
    waitWhileClusterExists(hoyaClient, 30000);
    //give yarn some time to notice
    sleep(2000)
    describe("final listing")
    lsJavaProcesses();
    ApplicationReport report = hoyaClient.applicationReport
    assert report.yarnApplicationState == YarnApplicationState.FAILED;



    clusterActionFreeze(hoyaClient, clustername)
  }


}
