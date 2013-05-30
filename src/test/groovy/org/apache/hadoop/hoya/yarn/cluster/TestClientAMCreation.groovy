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
import org.apache.hadoop.hoya.yarn.KeysForTests
import org.apache.hadoop.hoya.yarn.client.ClientArgs
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@Commons
class TestClientAMCreation extends YarnMiniClusterTestBase {

  @Test
  public void testAMCreation() throws Throwable {
    createCluster("testYARNClusterCreation",new YarnConfiguration(), 1)
    String rmAddr = getRMAddr();
    log.info("RM address = $rmAddr")
    ServiceLauncher launcher = launchHoyaClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.getConfig()),
        ClientArgs.ACTION_CREATE,
        "testAMCreations",
        CommonArgs.ARG_MIN, "1",
        CommonArgs.ARG_MAX, "1",
        ClientArgs.ARG_MANAGER, getRMAddr(),
        CommonArgs.ARG_USER, USERNAME,
        ClientArgs.ARG_WAIT, WAIT_TIME_ARG
    )
    assert launcher.serviceExitCode == 0
    HoyaClient hoyaClient = (HoyaClient)launcher.service
    
  }
  
}
