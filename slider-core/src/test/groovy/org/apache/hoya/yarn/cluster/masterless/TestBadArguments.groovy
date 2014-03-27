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

package org.apache.hoya.yarn.cluster.masterless

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hoya.HoyaExitCodes
import org.apache.hoya.HoyaKeys
import org.apache.hoya.HoyaXmlConfKeys
import org.apache.hoya.api.RoleKeys
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.service.launcher.ServiceLaunchException
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

@CompileStatic
@Slf4j

class TestBadArguments extends HBaseMiniClusterTestBase {

  @Test
  public void testBadAMHeap() throws Throwable {
    String clustername = "test_bad_am_heap"
    createMiniCluster(clustername, getConfiguration(), 1, true)

    describe "verify that bad Java heap options are picked up"

    try {
      ServiceLauncher launcher = createHoyaCluster(clustername,
           [
               (HBaseKeys.ROLE_MASTER): 0,
               (HBaseKeys.ROLE_WORKER): 0,
           ],
           [
               Arguments.ARG_COMP_OPT, HoyaKeys.COMPONENT_AM, RoleKeys.JVM_HEAP, "invalid",
           ],
           true,
           false,
           [:])
      HoyaClient hoyaClient = (HoyaClient) launcher.service
      addToTeardown(hoyaClient);

      ApplicationReport report = waitForClusterLive(hoyaClient)
      assert report.yarnApplicationState == YarnApplicationState.FAILED
      
    } catch (ServiceLaunchException e) {
      assertExceptionDetails(e, HoyaExitCodes.EXIT_YARN_SERVICE_FAILED)
    }
    
  }

  /**
   * Test disabled because YARN queues don't get validated in the mini cluster
   * @throws Throwable
   */
  public void DisabledtestBadYarnQueue() throws Throwable {
    String clustername = "test_bad_yarn_queue"
    createMiniCluster(clustername, getConfiguration(), 1, true)

    describe "verify that bad Java heap options are picked up"

    try {
      ServiceLauncher launcher = createHoyaCluster(clustername,
           [
               (HBaseKeys.ROLE_MASTER): 0,
               (HBaseKeys.ROLE_WORKER): 0,
           ],
           [
               Arguments.ARG_DEFINE,
               HoyaXmlConfKeys.KEY_YARN_QUEUE + "=noqueue"
           ],
           true,
           false,
           [:])
      HoyaClient hoyaClient = (HoyaClient) launcher.service
      addToTeardown(hoyaClient);

      ApplicationReport report = waitForClusterLive(hoyaClient)
      assert report.yarnApplicationState == YarnApplicationState.FAILED
      
    } catch (ServiceLaunchException e) {
      assertExceptionDetails(e, HoyaExitCodes.EXIT_YARN_SERVICE_FAILED)
    }
    
  }

}
