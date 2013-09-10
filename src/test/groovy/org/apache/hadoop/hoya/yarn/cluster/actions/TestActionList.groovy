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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.yarn.HoyaActions
import org.apache.hadoop.hoya.yarn.client.ClientArgs
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Before
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@CompileStatic
@Slf4j

class TestActionList extends HBaseMiniClusterTestBase {

  @Before
  public void setup() {
    createMiniCluster("testActionList", createConfiguration(), 1, false)
  }
  
  @Test
  public void testListThisUserNoClusters() throws Throwable {
    log.info("RM address = ${RMAddr}")
    ServiceLauncher launcher = launchHoyaClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            HoyaActions.ACTION_LIST,
            ClientArgs.ARG_MANAGER, RMAddr
        ]
    )
    assert launcher.serviceExitCode == 0
    HoyaClient hoyaClient = (HoyaClient) launcher.service
  }
  
  @Test
  public void testListAllUsersNoClusters() throws Throwable {
    log.info("RM address = ${RMAddr}")
    ServiceLauncher launcher = launchHoyaClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            HoyaActions.ACTION_LIST,
            ClientArgs.ARG_MANAGER, RMAddr,
            ClientArgs.ARG_USER,""
        ]
    )
    assert launcher.serviceExitCode == 0
  }

  @Test
  public void testListLiveCluster() throws Throwable {
    //launch the cluster
    String clustername = "testListLiveCluster"
    ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, false)
    ApplicationReport report = waitForClusterLive((HoyaClient) launcher.service)

    //now list
    launcher = launchHoyaClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            HoyaActions.ACTION_LIST,
        ]
    )
    assert launcher.serviceExitCode == 0
    //now look for the explicit sevice
    
    //do the low level operations to get a better view of what is going on 
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    ApplicationReport instance = hoyaClient.findInstance(hoyaClient.getUsername(),
                                                         clustername)
    assert instance != null
    log.info(instance.toString())

    //now list with the named cluster
    launcher = launchHoyaClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            HoyaActions.ACTION_LIST, clustername
        ]
    )

  }



  @Test
  public void testListMissingCluster() throws Throwable {
    describe("create exec the status command against an unknown cluster")
    //launch fake master
    //launch the cluster
    //exec the status command
    ServiceLauncher launcher
    try {
      launcher = launchHoyaClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              HoyaActions.ACTION_LIST,
              "testStatusMissingCluster"
          ]
      )
      fail("expected an exception, got a status code " + launcher.serviceExitCode)
    } catch (HoyaException e) {
      assert e.exitCode == HoyaExitCodes.EXIT_UNKNOWN_HOYA_CLUSTER
    }
  }


}
