/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hoya.funtest.hbase

import org.apache.hoya.funtest.framework.CommandTestBase
import org.apache.hoya.funtest.framework.SliderShell
import org.apache.hoya.providers.hbase.HBaseClientProvider
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.yarn.Arguments
import org.junit.Before

import static org.apache.hoya.funtest.framework.FuntestProperties.*

/**
 * Anything specific to HBase tests
 */
abstract class HBaseCommandTestBase extends CommandTestBase {

  @Before 
  public void verifyPreconditions() {
    assumeHBaseTestsEnabled()
    getRequiredConfOption(HOYA_CONFIG, KEY_TEST_HBASE_TAR)
    getRequiredConfOption(HOYA_CONFIG, KEY_TEST_HBASE_APPCONF)

  }

  /**
   * Create an hbase cluster -this patches in the relevant attributes
   * for the HBase cluster
   * @param name name
   * @param masters no. of master nodes
   * @param workers no. of region servers
   * @param argsList list of arguments
   * @param clusterOps map of cluster options
   * @return the role map -for use when waiting for the cluster to go live
   */
  public Map<String, Integer> createHBaseCluster(
      String name,
      int masters,
      int workers,
      List<String> argsList,
      Map<String, String> clusterOps) {
    Map<String, Integer> roleMap = [
        (HBaseKeys.ROLE_MASTER): masters,
        (HBaseKeys.ROLE_WORKER): workers
    ]

    argsList << Arguments.ARG_IMAGE <<
    HOYA_CONFIG.getTrimmed(KEY_TEST_HBASE_TAR)

    argsList << Arguments.ARG_CONFDIR <<
    HOYA_CONFIG.getTrimmed(KEY_TEST_HBASE_APPCONF)

    argsList << Arguments.ARG_PROVIDER << HBaseKeys.PROVIDER_HBASE
    
    SliderShell shell = createHoyaCluster(
        name,
        roleMap,
        argsList,
        true,
        clusterOps
    )
    return roleMap
  }


  public String getDescription() {
    return "Create an HBase"
  }

  public int getWorkerPortAssignment() {
    return 0
  }

  public int getMasterPortAssignment() {
    return 0
  }

}
