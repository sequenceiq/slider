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

import groovy.util.logging.Commons
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.hoya.yarn.client.ClientArgs
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
//@CompileStatic
class TestDestroyMasterlessAM extends YarnMiniClusterTestBase {

  @Test
  public void testDestroyMasterlessAM() throws Throwable {
    String clustername = "TestDestroyMasterlessAM"
    createMiniCluster(clustername, createConfiguration(), 1, true)

    describe "create a masterless AM, stop it, try to create" +
             "a second cluster with the same name, destroy it, try a third time"

    ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, true)
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);

    clusterActionFreeze(hoyaClient, clustername)
    waitForAppToFinish(hoyaClient)
    
    //now try to create instance #2, and expect an in-use failure
    try {
      createMasterlessAM(clustername, 0, false, false)
      fail("expected a failure, got an AM")
    } catch (HoyaException e) {
      assertExceptionDetails(e,
                             EXIT_BAD_CLUSTER_STATE,
                             HoyaClient.E_ALREADY_EXISTS)
    }

    //now: destroy it
    int exitCode = hoyaClient.actionDestroy(clustername);
    assert 0 == exitCode
    
    
    //expect thaw to now fail
    try {
      launcher = launch(HoyaClient,
                        createConfiguration(),
                        [
                            CommonArgs.ACTION_THAW,
                            clustername,
                            ClientArgs.ARG_FILESYSTEM, fsDefaultName,
                            ClientArgs.ARG_MANAGER, RMAddr,
                        ])
      assert launcher.serviceExitCode == HoyaExitCodes.EXIT_UNKNOWN_HOYA_CLUSTER
    } catch (HoyaException e) {
      assertExceptionDetails(e,
                             HoyaExitCodes.EXIT_UNKNOWN_HOYA_CLUSTER,
                             HoyaClient.E_UNKNOWN_CLUSTER)
    }

      //and create a new cluster
    launcher = createMasterlessAM(clustername, 0, false, false)
    HoyaClient cluster2 = launcher.service as HoyaClient
    
    //try to destroy it while live
    try {
      int ec = cluster2.actionDestroy(clustername)
      fail("expected a failure from the destroy, got error code $ec")
    } catch (HoyaException e) {
      assertExceptionDetails(e,
                             HoyaExitCodes.EXIT_BAD_CLUSTER_STATE,
                             HoyaClient.E_CLUSTER_RUNNING)
    }
    
    //and try to destroy a completely different cluster just for the fun of it
    assert 0 == hoyaClient.actionDestroy("no-cluster-of-this-name")
  }

  @Test
  public void testDestroyNonexistentCluster() throws Throwable {
    String clustername = "TestDestroyMasterlessAM"
    createMiniCluster(clustername, createConfiguration(), 1, true)
    ServiceLauncher launcher = launchHoyaClientAgainstMiniMR(
              createConfiguration(),
              [
                  CommonArgs.ACTION_DESTROY,
                  "no-cluster-of-this-name",
                  ClientArgs.ARG_FILESYSTEM, fsDefaultName,
              ])
    assert launcher.serviceExitCode == 0
  }


}
