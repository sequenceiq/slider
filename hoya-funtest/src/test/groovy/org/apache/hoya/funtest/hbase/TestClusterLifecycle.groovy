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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.service.launcher.LauncherExitCodes
import org.apache.hoya.HoyaExitCodes
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.funtest.framework.HoyaCommandTestBase
import org.apache.hoya.funtest.framework.HoyaFuntestProperties
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.HoyaActions
import org.apache.hoya.yarn.client.HoyaClient
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

@CompileStatic
@Slf4j
public class TestClusterLifecycle extends HoyaCommandTestBase
    implements HoyaFuntestProperties, Arguments, HoyaExitCodes {


  static String CLUSTER = "test_cluster_lifecycle"


  @BeforeClass
  public static void prepareCluster() {
    ensureClusterDestroyed(CLUSTER)
  }

  @AfterClass
  public static void destroyCluster() {
    ensureClusterDestroyed(CLUSTER)
  }

  @Test
  public void testClusterLifecycle() throws Throwable {

    describe "Walk a 0-role Hoya cluster through its lifecycle"

    Map<String, Integer> roleMap = [
        (HBaseKeys.ROLE_MASTER): 0,
        (HBaseKeys.ROLE_WORKER): 0,
    ]

    createHoyaCluster(
        CLUSTER,
        roleMap,
        [],
        true,
        [:])
    assert clusterFS.exists(
        new Path(clusterFS.homeDirectory, ".hoya/cluster/$CLUSTER"))

// assert it exists
    exists(0, CLUSTER)

    //destroy will fail in use

    destroy(EXIT_CLUSTER_IN_USE, CLUSTER)

    //thaw will fail as cluster is in use
    thaw(EXIT_CLUSTER_IN_USE, CLUSTER)

    //it's still there
    exists(0, CLUSTER)

    //listing the cluster will succeed
    list(0, CLUSTER)

    //simple status
    status(0, CLUSTER)

    //now status to a temp file
    File jsonStatus = File.createTempFile("hoya", ".json")
    try {
      hoya(0,
           [
               HoyaActions.ACTION_STATUS, CLUSTER,
               ARG_OUTPUT, jsonStatus.canonicalPath
           ])

      assert jsonStatus.exists()
      ClusterDescription cd = ClusterDescription.fromFile(jsonStatus)

      assert CLUSTER == cd.name

      log.info(cd.toJsonString())

      getConf(0, CLUSTER)

      //get a hoya client against the cluster
      HoyaClient hoyaClient = bondToCluster(HOYA_CONFIG, CLUSTER)
      ClusterDescription cd2 = hoyaClient.getClusterDescription()
      assert CLUSTER == cd2.name

      log.info("Connected via HoyaClient {}", hoyaClient.toString())

      //freeze
      hoya(0, [
          HoyaActions.ACTION_FREEZE, CLUSTER,
          ARG_WAIT, Integer.toString(FREEZE_WAIT_TIME),
          ARG_MESSAGE, "freeze-in-testHBaseCreateCluster"
      ])

      //cluster exists if you don't want it to be live
      exists(0, CLUSTER, false)
      // condition returns false if it is required to be live
      exists(LauncherExitCodes.EXIT_FALSE, CLUSTER, true)



      hoya(0,
           [
               HoyaActions.ACTION_THAW, CLUSTER,
               ARG_WAIT, Integer.toString(THAW_WAIT_TIME)
           ])
      exists(0, CLUSTER)
      hoya(0, [
          HoyaActions.ACTION_FREEZE, CLUSTER,
          ARG_FORCE,
          ARG_WAIT, Integer.toString(FREEZE_WAIT_TIME),
          ARG_MESSAGE, "forced-freeze-in-test"
      ])
      //cluster exists if you don't want it to be live
      exists(0, CLUSTER, false)
      // condition returns false if it is required to be live
      exists(LauncherExitCodes.EXIT_FALSE, CLUSTER, true)

      destroy(0, CLUSTER)

      //cluster now missing
      exists(HoyaExitCodes.EXIT_UNKNOWN_HOYA_CLUSTER, CLUSTER)

    } finally {
      jsonStatus.delete()
    }


  }


}
