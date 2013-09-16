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
import org.apache.hadoop.hoya.HoyaKeys
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.hoya.yarn.ZKIntegration
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * Create a master against the File:// fs
 */
@CompileStatic
@Slf4j

class TestFailingHBaseMasterCommand extends HBaseMiniClusterTestBase {


  public static final int AM_START_TO_FAIL_TIME = 30000

  @Test
  public void testFailingHBaseMasterCommand() throws Throwable {
    describe "create a cluster, exec an unsupported hbase version command"

    String clustername = "TestFailingHBaseMasterCommand"
    createMiniCluster(clustername, createConfiguration(), 1, true)
    ServiceLauncher launcher = createHBaseCluster(clustername,
                        0,
                        [ CommonArgs.ARG_OPTION, HoyaKeys.OPTION_HOYA_MASTER_COMMAND, "unknown-command" ],
                        true,
                        true)
    assert launcher.serviceExitCode == 0
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);

    ApplicationReport report = waitForAppToFinish(hoyaClient);
    if (report==null) {
      fail("Timeout: application did not fail");
    }
    assert report.finalApplicationStatus == FinalApplicationStatus.FAILED;
    
  }

}
