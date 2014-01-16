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

package org.apache.hoya.yarn.providers.hbase.build

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.service.launcher.LauncherExitCodes
import org.apache.hoya.HoyaExitCodes
import org.apache.hoya.exceptions.HoyaException
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.yarn.HoyaActions
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

@CompileStatic
@Slf4j

class TestBuildClusterM1W5 extends HBaseMiniClusterTestBase {

  @Test
  public void testBuildCluster() throws Throwable {
    String clustername = "test_build_cluster_m1_w5"
    createMiniCluster(clustername, getConfiguration(), 1, true)

    describe "verify that a build cluster is created but not started"

    ServiceLauncher launcher = createOrBuildHoyaCluster(
        HoyaActions.ACTION_BUILD,
        clustername,
        [
            (HBaseKeys.ROLE_MASTER): 1,
            (HBaseKeys.ROLE_WORKER): 5,
        ],
        [],
        true,
        false,
        [:])
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);

    //verify that exists(live) is now false
    assert LauncherExitCodes.EXIT_FALSE ==
           hoyaClient.actionExists(clustername, true)

    //but the cluster is still there for the default
    assert 0 == hoyaClient.actionExists(clustername, false)


    ApplicationReport report = hoyaClient.findInstance(clustername)
    assert report == null;

    //and a second attempt will fail as the cluster now exists
    try {
      createOrBuildHoyaCluster(
          HoyaActions.ACTION_BUILD,
          clustername,
          [
              (HBaseKeys.ROLE_MASTER): 1,
              (HBaseKeys.ROLE_WORKER): 3,
          ],
          [],
          true,
          false,
          [:])
    } catch (HoyaException e) {
      assert e.exitCode == HoyaExitCodes.EXIT_CLUSTER_EXISTS
    }
  }

}
