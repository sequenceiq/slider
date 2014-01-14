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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hoya.HoyaExitCodes
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.funtest.framework.HoyaCommandTestBase
import org.apache.hoya.funtest.framework.HoyaTestProperties
import org.apache.hoya.funtest.framework.PortAssignments
import org.apache.hoya.providers.hbase.HBaseKeys
import static org.apache.hoya.testtools.HBaseTestUtils.*
import org.apache.hoya.testtools.HBaseTestUtils
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.HoyaActions
import org.apache.hoya.yarn.client.HoyaClient
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

@CompileStatic
@Slf4j
public class TestFunctionalHBaseCluster extends HoyaCommandTestBase
    implements HoyaTestProperties, Arguments, HoyaExitCodes {


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
    hoya(0,
         [
             HoyaActions.ACTION_CREATE,
             CLUSTER,
             ARG_ZKHOSTS,
             HOYA_CONFIG.get(KEY_HOYA_TEST_ZK_HOSTS, DEFAULT_HOYA_ZK_HOSTS),
             ARG_IMAGE,
             HOYA_CONFIG.get(KEY_HOYA_TEST_HBASE_TAR),
             ARG_CONFDIR,
             HOYA_CONFIG.get(KEY_HOYA_TEST_HBASE_APPCONF),
             ARG_ROLE, HBaseKeys.ROLE_MASTER, "1",
             ARG_ROLE, HBaseKeys.ROLE_WORKER, "1",
             ARG_ROLEOPT, HBaseKeys.ROLE_MASTER, "app.infoport",
             Integer.toString(PortAssignments._testHBaseCreateCluster),
             ARG_ROLEOPT, HBaseKeys.ROLE_WORKER, "app.infoport",
             Integer.toString(PortAssignments._testHBaseCreateCluster2),
             ARG_WAIT, Integer.toString(THAW_WAIT_TIME)
         ])




    //get a hoya client against the cluster
    HoyaClient hoyaClient = bondToCluster(HOYA_CONFIG, CLUSTER)
    ClusterDescription cd2 = hoyaClient.getClusterDescription()
    assert CLUSTER == cd2.name

    log.info("Connected via HoyaClient {}", hoyaClient.toString())

    //wait for the role counts to be reached
    waitForRoleCount(hoyaClient, 
                     [
                       (HBaseKeys.ROLE_MASTER): 1,
                       (HBaseKeys.ROLE_WORKER): 1
                     ],
                     THAW_WAIT_TIME)

    Configuration clientConf = HBaseTestUtils.createHBaseConfiguration(hoyaClient)
    assertHBaseMasterFound(clientConf)
    waitForHBaseRegionServerCount(hoyaClient,CLUSTER, 1, HBASE_LAUNCH_WAIT_TIME)
    

   // role count good, let's talk HBase
   


  }


}
