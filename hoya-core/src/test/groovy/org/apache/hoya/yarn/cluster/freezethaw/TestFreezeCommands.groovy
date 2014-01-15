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

package org.apache.hoya.yarn.cluster.freezethaw

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hoya.exceptions.HoyaException
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.HoyaActions
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@CompileStatic
@Slf4j

class TestFreezeCommands extends HBaseMiniClusterTestBase {

  @Test
  public void testFreezeCommands() throws Throwable {
    String clustername = "test_freeze_commands"
    YarnConfiguration conf = getConfiguration()
    createMiniCluster(clustername, conf, 1, 1, 1, true, true)

    describe "create a masterless AM, freeze it, try to freeze again"

    ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, true);
    addToTeardown(launcher.service as HoyaClient);

    
    log.info("ListOp")
    assertSucceeded(execHoyaCommand(conf,
              [HoyaActions.ACTION_LIST,clustername]))
    
    log.info("First Freeze command");
    ServiceLauncher freezeCommand = execHoyaCommand(conf,
                          [HoyaActions.ACTION_FREEZE, clustername,
                            Arguments.ARG_WAIT, waitTimeArg]);
    assertSucceeded(freezeCommand)

    log.info("Second Freeze command");

    ServiceLauncher freeze2 = execHoyaCommand(conf,
                                [
                                    HoyaActions.ACTION_FREEZE, clustername,
                                    Arguments.ARG_WAIT, waitTimeArg
                                ]);
    assertSucceeded(freeze2)

    log.info("First Exists");

    //assert there is no running cluster
    try {
      ServiceLauncher exists1 = launchHoyaClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          [
              HoyaActions.ACTION_EXISTS, clustername,
              Arguments.ARG_FILESYSTEM, fsDefaultName
          ],
          )
      assert 0 != exists1.serviceExitCode;
    } catch (HoyaException e) {
      assertUnknownClusterException(e)
    }

    log.info("First Thaw");

    ServiceLauncher thawCommand = execHoyaCommand(conf,
                          [
                              HoyaActions.ACTION_THAW, clustername,
                              Arguments.ARG_WAIT, waitTimeArg,
                              Arguments.ARG_FILESYSTEM, fsDefaultName
                          ]);
    assertSucceeded(thawCommand)
    assertSucceeded(execHoyaCommand(conf,
                  [HoyaActions.ACTION_LIST, clustername]))
    assertSucceeded(execHoyaCommand(conf,
                  [HoyaActions.ACTION_EXISTS, clustername]))

    log.info("Freeze 3");

    ServiceLauncher freeze3 = execHoyaCommand(conf,
                [
                    HoyaActions.ACTION_FREEZE, clustername,
                    Arguments.ARG_WAIT, waitTimeArg
                ]);
    assertSucceeded(freeze3)

    log.info("thaw2");
    ServiceLauncher thaw2 = execHoyaCommand(conf,
                [
                    HoyaActions.ACTION_THAW, clustername,
                    Arguments.ARG_WAIT, waitTimeArg,
                    Arguments.ARG_FILESYSTEM, fsDefaultName
                ]);
    assert 0 == thaw2.serviceExitCode;
    assertSucceeded(thaw2)

    try {
      log.info("thaw3 - should fail");
      ServiceLauncher thaw3 = execHoyaCommand(conf,
                [
                    HoyaActions.ACTION_THAW, clustername,
                    Arguments.ARG_WAIT, waitTimeArg,
                    Arguments.ARG_FILESYSTEM, fsDefaultName
                ]);
      assert 0 != thaw3.serviceExitCode;
    } catch (HoyaException e) {
      assertFailureClusterInUse(e);
    }

    //destroy should fail

    log.info("destroy1");

    try {
      ServiceLauncher destroy1 = execHoyaCommand(conf,
                                                 [HoyaActions.ACTION_DESTROY, clustername,
                                                     Arguments.ARG_FILESYSTEM, fsDefaultName]);
      fail("expected a failure from the destroy, got error code ${destroy1.serviceExitCode}");
    } catch (HoyaException e) {
      assertFailureClusterInUse(e);
    }
    log.info("freeze4");
    
    //kill -19 the process to hang it, then force kill
    killHoyaAM(SIGSTOP)

    ServiceLauncher freeze4 = execHoyaCommand(conf,
                                              [
                                                  HoyaActions.ACTION_FREEZE, clustername,
                                                  Arguments.ARG_FORCE,
                                                  Arguments.ARG_WAIT, waitTimeArg,
                                              ]);
    assertSucceeded(freeze4)

    log.info("destroy2");
    ServiceLauncher destroy2 = execHoyaCommand(conf,
                                               [
                                                   HoyaActions.ACTION_DESTROY, clustername,
                                                   Arguments.ARG_FILESYSTEM, fsDefaultName,
                                               ]);
    assertSucceeded(destroy2)

  }


}
