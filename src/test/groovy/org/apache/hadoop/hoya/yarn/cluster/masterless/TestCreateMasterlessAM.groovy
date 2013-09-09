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
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Assume
import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@CompileStatic
@Slf4j

class TestCreateMasterlessAM extends HBaseMiniClusterTestBase {

  @Test
  public void testCreateMasterlessAM() throws Throwable {
    createMiniCluster("TestCreateMasterlessAM", createConfiguration(), 1, true)

    describe "create a masterless AM then get the service and look it up via the AM"

    //launch fake master
    String clustername = "TestCreateMasterlessAM"
    ServiceLauncher launcher
    launcher = createMasterlessAM(clustername, 0, true, false)
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);

    ApplicationReport report = waitForClusterLive(hoyaClient)
    logReport(report)
    List<ApplicationReport> apps = hoyaClient.applications;
    String username = hoyaClient.username
    describe("list of all applications")
    logApplications(apps)
    describe("apps of user $username")
    List<ApplicationReport> userInstances = hoyaClient.listHoyaInstances(username)
    logApplications(userInstances)
    assert userInstances.size() == 1
    describe("named app $clustername")
    ApplicationReport instance = hoyaClient.findInstance(username, clustername)
    logReport(instance)
    assert instance != null

    //now kill that cluster
    assert 0 == clusterActionFreeze(hoyaClient, clustername)
    //list it & See if it is still there
    ApplicationReport oldInstance = hoyaClient.findInstance(username, clustername)
    assert oldInstance != null
    assert oldInstance.yarnApplicationState >= YarnApplicationState.FINISHED

    //create another AM
    launcher = createMasterlessAM(clustername, 0, true, true)
    hoyaClient = (HoyaClient) launcher.service
    ApplicationId i2AppID = hoyaClient.applicationId

    //expect 2 in the list
    userInstances = hoyaClient.listHoyaInstances(username)
    logApplications(userInstances)
    assert userInstances.size() == 2

    //but when we look up an instance, we get the new App ID
    ApplicationReport instance2 = hoyaClient.findInstance(username, clustername)
    assert i2AppID == instance2.applicationId

    describe("Creating instance #3")
    //now try to create instance #3, and expect an in-use failure
    try {
      createMasterlessAM(clustername, 0, true, true)
      fail("expected a failure, got a masterless AM")
    } catch (HoyaException e) {
      assert e.exitCode == HoyaExitCodes.EXIT_BAD_CLUSTER_STATE
      assert e.toString().contains(HoyaClient.E_CLUSTER_RUNNING)
    }

    describe("Stopping instance #2")

    //now stop that cluster
    assert 0 == clusterActionFreeze(hoyaClient, clustername)

    logApplications(hoyaClient.listHoyaInstances(username))
    
    //verify it is down
    ApplicationReport reportFor = hoyaClient.getApplicationReport(i2AppID)
    
    //downgrade this to a fail
    Assume.assumeTrue(YarnApplicationState.FINISHED <= report.yarnApplicationState)
    assert YarnApplicationState.FINISHED <= report.yarnApplicationState


    ApplicationReport instance3 = hoyaClient.findInstance(username, clustername)
    assert instance3.yarnApplicationState >= YarnApplicationState.FINISHED


  }


}
