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
import groovy.util.logging.Commons
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.yarn.HoyaActions
import org.apache.hadoop.hoya.yarn.client.ClientArgs
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@CompileStatic
@Commons
class TestFreezeCommands extends YarnMiniClusterTestBase {

  @Test
  public void testCreateFreezeFreezeThawFreezeMasterlessAM() throws Throwable {
    String clustername = "TestFreezeCommands"
    YarnConfiguration conf = createConfiguration()
    createMiniCluster(clustername, conf, 1, 1, 1, true, true)

    describe "create a masterless AM, freeze it, try to freeze again"

    ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, true);

    log.info("First Freeze command");
    ServiceLauncher freezeCommand = execHoyaCommand(conf,
                          [HoyaActions.ACTION_FREEZE, clustername,
                            ClientArgs.ARG_WAIT, waitTimeArg]);
    assert 0==freezeCommand.serviceExitCode;

    log.info("Second Freeze command");

    ServiceLauncher freeze2 = execHoyaCommand(conf,
                                [
                                    HoyaActions.ACTION_FREEZE, clustername,
                                    ClientArgs.ARG_WAIT, waitTimeArg
                                ]);
    assert 0 == freeze2.serviceExitCode;

    log.info("First Exists");

    //assert there is no running cluster
    try {
      ServiceLauncher exists1 = launchHoyaClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          [
              HoyaActions.ACTION_EXISTS, clustername,
              ClientArgs.ARG_WAIT, waitTimeArg,
              ClientArgs.ARG_FILESYSTEM, fsDefaultName
          ],
          )
      assert 0 != exists1.serviceExitCode;
    } catch (HoyaException e) {
      assert e.exitCode == HoyaExitCodes.EXIT_UNKNOWN_HOYA_CLUSTER
    }

    log.info("First Thaw");

    ServiceLauncher thawCommand = execHoyaCommand(conf,
                          [
                              HoyaActions.ACTION_THAW, clustername,
                              ClientArgs.ARG_WAIT, waitTimeArg,
                              ClientArgs.ARG_FILESYSTEM, fsDefaultName
                          ]);
    assert 0 == thawCommand.serviceExitCode;

    log.info("Freeze 3");

    ServiceLauncher freeze3 = execHoyaCommand(conf,
                                              [
                                                  HoyaActions.ACTION_FREEZE, clustername,
                                                  ClientArgs.ARG_WAIT, waitTimeArg
                                              ]);
    assert 0 == freeze3.serviceExitCode;

    log.info("thaw2");
    ServiceLauncher thaw2 = execHoyaCommand(conf,
                                            [
                                                HoyaActions.ACTION_THAW, clustername,
                                                ClientArgs.ARG_WAIT, waitTimeArg,
                                                ClientArgs.ARG_FILESYSTEM, fsDefaultName
                                            ]);
    assert 0 == thaw2.serviceExitCode;

    try {
      log.info("thaw3 - should fail");
      ServiceLauncher thaw3 = execHoyaCommand(conf,
                                              [
                                                  HoyaActions.ACTION_THAW, clustername,
                                                  ClientArgs.ARG_WAIT, waitTimeArg,
                                                  ClientArgs.ARG_FILESYSTEM, fsDefaultName
                                              ]);
      assert 0 != thaw3.serviceExitCode;
    } catch (HoyaException e) {
      assertExceptionDetails(e,
                             HoyaExitCodes.EXIT_BAD_CLUSTER_STATE,
                             HoyaClient.E_CLUSTER_RUNNING);
    }

    //destroy should fail

    log.info("destroy1");

    try {
      ServiceLauncher destroy1 = execHoyaCommand(conf,
                                                 [HoyaActions.ACTION_DESTROY, clustername,
                                                     ClientArgs.ARG_FILESYSTEM, fsDefaultName]);
      fail("expected a failure from the destroy, got error code ${destroy1.serviceExitCode}");
    } catch (HoyaException e) {
      assertExceptionDetails(e,
                             HoyaExitCodes.EXIT_BAD_CLUSTER_STATE,
                             HoyaClient.E_CLUSTER_RUNNING);
    }
    log.info("freeze4");

    ServiceLauncher freeze4 = execHoyaCommand(conf,
                                              [
                                                  HoyaActions.ACTION_FREEZE, clustername,
                                                  ClientArgs.ARG_WAIT, waitTimeArg
                                              ]);
    assert 0 == freeze4.serviceExitCode;

    log.info("destroy2");
    ServiceLauncher destroy2 = execHoyaCommand(conf,
                                               [
                                                   HoyaActions.ACTION_DESTROY, clustername,
                                                   ClientArgs.ARG_FILESYSTEM, fsDefaultName,
                                                   ClientArgs.ARG_WAIT, waitTimeArg
                                               ]);
    assert 0 == destroy2.serviceExitCode;

  }

}
