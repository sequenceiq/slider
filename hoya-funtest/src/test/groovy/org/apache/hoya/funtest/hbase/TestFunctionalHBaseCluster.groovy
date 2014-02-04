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
import org.apache.hoya.api.RoleKeys
import org.apache.hoya.funtest.framework.HoyaFuntestProperties
import static org.apache.hoya.funtest.framework.HoyaFuntestProperties.KEY_HOYA_TEST_HBASE_ENABLED
import org.apache.hoya.tools.ConfigHelper
import org.junit.After
import org.junit.Before

import static org.apache.hoya.testtools.HBaseTestUtils.*

import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.client.HoyaClient
import org.junit.Test

import static org.apache.hoya.providers.hbase.HBaseKeys.*

@CompileStatic
@Slf4j
public class TestFunctionalHBaseCluster extends HBaseCommandTestBase
    implements HoyaFuntestProperties, Arguments, HoyaExitCodes {


  public static final String HBASE_HEAP = "256m"

  public String getClusterName() {
    return "test_functional_hbase_cluster"
  }

  @Before
  public void prepareCluster() {
    setupCluster(clusterName)
  }

  @After
  public void teardownCluster() {
    teardown(clusterName)
  }

  @Test
  public void testHBaseCreateCluster() throws Throwable {

    describe description

    int numWorkers = HOYA_CONFIG.getInt(KEY_HOYA_TEST_NUM_WORKERS, 
        DEFAULT_HOYA_NUM_WORKERS);

    def clusterpath = buildClusterPath(clusterName)
    assert !clusterFS.exists(clusterpath)
    Map<String, Integer> roleMap = createHBaseCluster(
        clusterName,
        1, numWorkers,
        [
            ARG_ROLEOPT, ROLE_MASTER, RoleKeys.APP_INFOPORT,
              Integer.toString(masterPortAssignment),
            ARG_ROLEOPT, ROLE_MASTER, RoleKeys.JVM_HEAP, HBASE_HEAP,
            ARG_ROLEOPT, ROLE_WORKER, RoleKeys.APP_INFOPORT,
              Integer.toString(workerPortAssignment),
            ARG_ROLEOPT, ROLE_WORKER, RoleKeys.JVM_HEAP, HBASE_HEAP,
        ],
        [:]
    )

    //get a hoya client against the cluster
    HoyaClient hoyaClient = bondToCluster(HOYA_CONFIG, clusterName)
    ClusterDescription cd2 = hoyaClient.getClusterDescription()
    assert clusterName == cd2.name

    log.info("Connected via HoyaClient {} with {} workers", hoyaClient.toString(),
        numWorkers)

    //wait for the role counts to be reached
    waitForRoleCount(hoyaClient, roleMap, HBASE_LAUNCH_WAIT_TIME)

    Configuration clientConf = createHBaseConfiguration(hoyaClient)
    assertHBaseMasterFound(clientConf)
    waitForHBaseRegionServerCount(hoyaClient, clusterName,
                                  numWorkers, HBASE_LAUNCH_WAIT_TIME)

    clusterLoadOperations(clusterName, clientConf, numWorkers, roleMap, cd2)
  }


  public String getDescription() {
    return "Create a working HBase cluster $clusterName"
  }

  /**
   * Override point for any cluster load operations
   * @param clientConf
   * @param numWorkers
   */
  public void clusterLoadOperations(
      String clustername,
      Configuration clientConf,
      int numWorkers,
      Map<String, Integer> roleMap,
      ClusterDescription cd) {

    log.info("Client Configuration = " + ConfigHelper.dumpConfigToString(clientConf))
  }

}
