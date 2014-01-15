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

package org.apache.hoya.funtest.accumulo

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hoya.HoyaExitCodes
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.api.RoleKeys
import org.apache.hoya.funtest.framework.HoyaFuntestProperties
import org.apache.hoya.funtest.framework.PortAssignments
import org.apache.hoya.providers.accumulo.AccumuloConfigFileOptions
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.client.HoyaClient
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

import static org.apache.hoya.providers.accumulo.AccumuloKeys.ROLE_MONITOR
import static org.apache.hoya.testtools.HoyaTestUtils.describe
import static org.apache.hoya.testtools.HoyaTestUtils.waitForRoleCount

@CompileStatic
@Slf4j
public class TestFunctionalAccumuloCluster extends AccumuloCommandTestBase
    implements HoyaFuntestProperties, Arguments, HoyaExitCodes {


  static String CLUSTER = "test_functional_hbase_cluster"


  @BeforeClass
  public static void prepareCluster() {
    ensureClusterDestroyed(CLUSTER)
  }

  @AfterClass
  public static void destroyCluster() {
    ensureClusterDestroyed(CLUSTER)
  }

  @Test
  public void testHBaseCreateCluster() throws Throwable {

    describe "Create a working HBase cluster"


    Map<String, Integer> roleMap = [
        (HBaseKeys.ROLE_MASTER): 1,
        (HBaseKeys.ROLE_WORKER): 1,
    ]

    List<String> extraArgs = [
        ARG_ROLEOPT, HBaseKeys.ROLE_MASTER, "app.infoport",
        Integer.toString(PortAssignments._testHBaseCreateCluster),
        ARG_ROLEOPT, HBaseKeys.ROLE_WORKER, "app.infoport",
        Integer.toString(PortAssignments._testHBaseCreateCluster2),
        RoleKeys.APP_INFOPORT, AccumuloConfigFileOptions.MASTER_PORT_CLIENT_DEFAULT,
        ARG_ROLEOPT, ROLE_MONITOR, RoleKeys.APP_INFOPORT,
        AccumuloConfigFileOptions.MONITOR_PORT_CLIENT_DEFAULT
    ]

    createHoyaCluster(
        CLUSTER,
        roleMap,
        extraArgs,
        true,
        [:]
    )

    //get a hoya client against the cluster
    HoyaClient hoyaClient = bondToCluster(HOYA_CONFIG, CLUSTER)
    ClusterDescription cd2 = hoyaClient.getClusterDescription()
    assert CLUSTER == cd2.name

    log.info("Connected via HoyaClient {}", hoyaClient.toString())

    //wait for the role counts to be reached
    waitForRoleCount(hoyaClient, roleMap, HBASE_LAUNCH_WAIT_TIME)


  }

}
