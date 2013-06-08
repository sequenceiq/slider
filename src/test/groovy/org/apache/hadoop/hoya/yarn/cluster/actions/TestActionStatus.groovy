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
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.hoya.yarn.client.ClientArgs
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Before
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@Commons
class TestActionStatus extends YarnMiniClusterTestBase {

  @Before
  public void setup() {
    createMiniCluster("TestActionStatus", new YarnConfiguration(), 1, false)
  }
  
  @Test
  public void testStatusLiveCluster() throws Throwable {
    //launch fake master
    String clustername = "testStatusLiveCluster"
    
    //launch the cluster
    ServiceLauncher launcher = createMasterlessAM(clustername, 0)
    
    //now list
    launcher = launchHoyaClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            ClientArgs.ACTION_STATUS,
            clustername,
            ClientArgs.ARG_MANAGER, RMAddr,
            CommonArgs.ARG_USER, USERNAME
        ]
        
    )
    assert launcher.serviceExitCode == 0
    //now look for the explicit sevice
    
    //do the low level operations to get a better view of what is going on 
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    ApplicationReport instance = hoyaClient.actionStatus()
    assert instance != null
    log.info(instance.toString())

  }


}
