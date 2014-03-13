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
import org.apache.hoya.HoyaKeys
import org.apache.hoya.api.ClusterNode
import org.apache.hoya.exceptions.HoyaException
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.params.ActionEchoArgs
import org.apache.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationId
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

class TestCreateMasterlessAM extends HBaseMiniClusterTestBase {

  @Test
  public void testCreateMasterlessAM() throws Throwable {
    

    describe "create a masterless AM then get the service and look it up via the AM"

    //launch fake master
    String clustername = "test_create_masterless_am"
    createMiniCluster(clustername, getConfiguration(), 1, true)
    ServiceLauncher launcher
    launcher = createMasterlessAM(clustername, 0, true, false)
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);

    ApplicationReport report = waitForClusterLive(hoyaClient)
    logReport(report)
    List<ApplicationReport> apps = hoyaClient.applications;
    
    //get some of its status
    dumpClusterStatus(hoyaClient,"masterless application status")
    List<ClusterNode> clusterNodes = hoyaClient.listClusterNodesInRole(
        HoyaKeys.ROLE_HOYA_AM)
    assert clusterNodes.size() == 1

    ClusterNode masterNode = clusterNodes[0]
    log.info("Master node = ${masterNode}");

    List<ClusterNode> nodes
    String[] uuids = hoyaClient.listNodeUUIDsByRole(HoyaKeys.ROLE_HOYA_AM)
    assert uuids.length == 1;
    nodes = hoyaClient.listClusterNodes(uuids);
    assert nodes.size() == 1;
    describe "AM Node UUID=${uuids[0]}"

    nodes = listNodesInRole(hoyaClient, HoyaKeys.ROLE_HOYA_AM)
    assert nodes.size() == 1;
    nodes = listNodesInRole(hoyaClient, "")
    assert nodes.size() == 1;
    ClusterNode master = nodes[0]
    assert master.role == HoyaKeys.ROLE_HOYA_AM




    String username = hoyaClient.username
    def serviceRegistryClient = hoyaClient.serviceRegistryClient
    describe("list of all applications")
    logApplications(apps)
    describe("apps of user $username")
    List<ApplicationReport> userInstances = serviceRegistryClient.listInstances()
    logApplications(userInstances)
    assert userInstances.size() == 1
    describe("named app $clustername")
    ApplicationReport instance = serviceRegistryClient.findInstance(clustername)
    logReport(instance)
    assert instance != null

    //now kill that cluster
    assert 0 == clusterActionFreeze(hoyaClient, clustername)
    //list it & See if it is still there
    ApplicationReport oldInstance = serviceRegistryClient.findInstance(clustername)
    assert oldInstance != null
    assert oldInstance.yarnApplicationState >= YarnApplicationState.FINISHED

    //create another AM
    launcher = createMasterlessAM(clustername, 0, true, true)
    hoyaClient = (HoyaClient) launcher.service
    ApplicationId i2AppID = hoyaClient.applicationId

    //expect 2 in the list
    userInstances = serviceRegistryClient.listInstances()
    logApplications(userInstances)
    assert userInstances.size() == 2

    //but when we look up an instance, we get the new App ID
    ApplicationReport instance2 = serviceRegistryClient.findInstance(clustername)
    assert i2AppID == instance2.applicationId
    
    
    
    // do an echo here of a large string
    // Hadoop RPC couldn't handle strings > 32K chars, this
    // check here allows us to be confident that large JSON Reports are handled
    StringBuilder sb = new StringBuilder()
    for (int i = 0; i < 65536; i++) {
      sb.append(Integer.toString(i, 16))
    }
    ActionEchoArgs args = new ActionEchoArgs();
    args.message = sb.toString();
    def echoed = hoyaClient.actionEcho(clustername, args)
    assert echoed == args.message
    log.info("Successful echo of a text document ${echoed.size()} characters long")

    describe("Creating instance #3")
    //now try to create instance #3, and expect an in-use failure
    try {
      createMasterlessAM(clustername, 0, false, true)
      fail("expected a failure, got a masterless AM")
    } catch (HoyaException e) {
      assertFailureClusterInUse(e);
    }

    describe("Stopping instance #2")

    //now stop that cluster
    assert 0 == clusterActionFreeze(hoyaClient, clustername)

    logApplications(hoyaClient.listHoyaInstances(username))
    
    //verify it is down
    ApplicationReport reportFor = hoyaClient.getApplicationReport(i2AppID)
    
    //downgrade this to a fail
//    Assume.assumeTrue(YarnApplicationState.FINISHED <= report.yarnApplicationState)
    assert YarnApplicationState.FINISHED <= reportFor.yarnApplicationState


    ApplicationReport instance3 = serviceRegistryClient.findInstance(clustername)
    assert instance3.yarnApplicationState >= YarnApplicationState.FINISHED


  }


}
