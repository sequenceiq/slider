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
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.providers.hbase.HBaseKeys
import org.apache.hadoop.hoya.yarn.Arguments
import org.apache.hadoop.hoya.yarn.HoyaActions
import org.apache.hoya.funtest.framework.HoyaCommandTestBase
import org.apache.hoya.funtest.framework.HoyaTestProperties
import org.apache.hoya.funtest.framework.PortAssignments
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

@CompileStatic
@Slf4j
public class TestHBaseCreateCluster extends HoyaCommandTestBase
    implements HoyaTestProperties, Arguments, HoyaExitCodes {


  static String CLUSTER = "test_hbase_create_cluster"
  

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

    assert clusterFS.exists(
        new Path(clusterFS.homeDirectory, ".hoya/cluster/$CLUSTER"))

    // assert it exists
    exists(0, CLUSTER)
    
    //destroy will fail in use
    
    destroy(EXIT_CLUSTER_IN_USE, CLUSTER)
    
    //thaw will fail as cluster is in use
    thaw(EXIT_CLUSTER_IN_USE, CLUSTER)
    
    //listing the cluster will succeed
    list(0, CLUSTER)
    
    status(0, CLUSTER)
    
    getConf(0, CLUSTER)
    


    //freeze
    hoya(0, [
        HoyaActions.ACTION_FREEZE, CLUSTER,
        ARG_WAIT, Integer.toString(FREEZE_WAIT_TIME),
        ARG_MESSAGE, "freeze-in-testHBaseCreateCluster"
    ])

    exists(HoyaExitCodes.EXIT_UNKNOWN_HOYA_CLUSTER, CLUSTER)

/*

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
*/

    destroy(0, CLUSTER)


  }


}
