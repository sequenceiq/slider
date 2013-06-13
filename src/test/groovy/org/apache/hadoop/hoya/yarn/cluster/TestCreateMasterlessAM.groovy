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







package org.apache.hadoop.hoya.yarn.cluster

import groovy.util.logging.Commons
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.hoya.yarn.client.ClientArgs
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@Commons
class TestCreateMasterlessAM extends YarnMiniClusterTestBase {



  @Test
  public void testCreateMasterlessAM() throws Throwable {
    createMiniCluster("TestCreateMasterlessAM", new YarnConfiguration(), 1, true)
    
    describe "create a masteress AM then get the service and look it up via the AM"

    //launch fake master
    String clustername = "TestCreateMasterlessAM"
    String zk = microZKCluster.zkBindingString
    String hbaseHome = HBaseHome
    String rmAddr = RMAddr
    ServiceLauncher launcher
    launcher = createMasterlessAM(clustername, 0, true) 
    HoyaClient hoyaClient = (HoyaClient) launcher.service

    ApplicationReport report = waitForClusterLive(hoyaClient)
    logReport(report)
    List<ApplicationReport> apps = hoyaClient.applicationList;
    String username = hoyaClient.username
    describe("list of all applications")
    logApplications(apps)
    describe("apps of user $username")
    logApplications(hoyaClient.listHoyaInstances(username))
    describe("named app $clustername")
    ApplicationReport instance = hoyaClient.findInstance(username, clustername)
    logReport(instance)
    assert instance != null

  }


}
