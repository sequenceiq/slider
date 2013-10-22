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

import groovy.util.logging.Slf4j
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.yarn.Arguments
import org.apache.hadoop.hoya.yarn.client.ClientArgs
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Before
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
//@CompileStatic
@Slf4j
class TestActionStatus extends HBaseMiniClusterTestBase {

  @Before
  public void setup() {
    super.setup()
    createMiniCluster("TestActionStatus", createConfiguration(), 1, false)
  }

  @Test
  public void testStatusMissingCluster() throws Throwable {
    describe("create exec the status command against an unknown cluster")
    //launch fake master
    //launch the cluster
    //exec the status command
    try {
      ServiceLauncher launcher = launchHoyaClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              ClientArgs.ACTION_STATUS,
              "testStatusMissingCluster",
              Arguments.ARG_MANAGER, RMAddr
          ]
      )
      fail("expected an exception, got a status code " + launcher.serviceExitCode)
    } catch (HoyaException e) {
      assert e.exitCode == EXIT_UNKNOWN_HOYA_CLUSTER
    }
  }
  
  @Test
  public void testStatusLiveCluster() throws Throwable {
    describe("create a live cluster then exec the status command")
    //launch fake master
    String clustername = "testStatusLiveCluster"
    
    //launch the cluster
    ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, false)

    ApplicationReport report = waitForClusterLive((HoyaClient)launcher.service)

    //now exec the status command
    launcher = launchHoyaClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            ClientArgs.ACTION_STATUS,
            clustername,
            Arguments.ARG_MANAGER, RMAddr,
        ]
        
    )
    assert launcher.serviceExitCode == 0
    //now look for the explicit sevice
    
    //do the low level operations to get a better view of what is going on 
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    int status = hoyaClient.actionStatus(clustername)
    assert status == EXIT_SUCCESS
  }


}
