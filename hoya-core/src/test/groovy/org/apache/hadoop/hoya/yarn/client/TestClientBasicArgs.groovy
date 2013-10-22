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

package org.apache.hadoop.hoya.yarn.client

import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.hoya.yarn.Arguments
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hadoop.yarn.service.launcher.ServiceLauncherBaseTest
import org.junit.Test

/**
 * Test bad argument handling
 */
//@CompileStatic
class TestClientBasicArgs extends ServiceLauncherBaseTest {

  /**
   * help should print out help string and then succeed
   * @throws Throwable
   */
  @Test
  public void testHelp() throws Throwable {
    ServiceLauncher launcher = launch(HoyaClient,
                                      HoyaUtils.createConfiguration(),
                                      [ClientArgs.ACTION_HELP])
    assert 0 == launcher.serviceExitCode
  }
  /**
   * help should print out help string and then succeed
   * @throws Throwable
   */
  @Test
  public void testHelpWithHyphenArgs() throws Throwable {
    ServiceLauncher launcher = launch(HoyaClient,
                                      HoyaUtils.createConfiguration(),
                                      [
                                          ClientArgs.ACTION_HELP,
                                          Arguments.ARG_DEBUG,
                                          Arguments.ARG_IMAGE, "hdfs://users/bob/hbase0.94.tar.gz",
                                          Arguments.ARG_CONFDIR, "hdfs://users/bob/hoya/conf1"
                                      ])
    assert 0 == launcher.serviceExitCode

  }

  @Test
  public void testHelpActionOrdering() throws Throwable {
    ServiceLauncher launcher = launch(HoyaClient,
                                      HoyaUtils.createConfiguration(),
                                      [
                                          Arguments.ARG_DEBUG,
                                          ClientArgs.ACTION_HELP,
                                      ])
    assert 0 == launcher.serviceExitCode
  }

  @Test
  public void testHelpActionOrderingDualItemArg() throws Throwable {
    ServiceLauncher launcher = launch(HoyaClient,
                                      HoyaUtils.createConfiguration(),
                                      [
                                          Arguments.ARG_WAIT, "60",
                                          ClientArgs.ACTION_HELP,
                                      ])
    assert 0 == launcher.serviceExitCode
  }

}
