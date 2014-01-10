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
    implements HoyaTestProperties, Arguments {


  static String CLUSTER = "test_hbase_create_cluster"
  

  @BeforeClass
  public static void prepareCluster() {
    ensureClusterDestroyed(CLUSTER)
 }

  @AfterClass
  public static void destroyCluster() {
    destroy(CLUSTER)
  }
  
  //TODO
  
  @Test
  public void testHBaseCreateCluster() throws Throwable {

    int wait = HOYA_CONFIG.getInt(KEY_HOYA_WAIT_TIME, DEFAULT_HOYA_WAIT_TIME)
    
    describe "Create a working HBase cluster"
    hoya(0,
         [
             HoyaActions.ACTION_BUILD,
             CLUSTER,
             ARG_ZKHOSTS,
             HOYA_CONFIG.get(KEY_HOYA_TEST_ZK_HOSTS),
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
//             ARG_WAIT, Integer.toString(wait)

         ])

    assert clusterFS.exists(
        new Path(clusterFS.homeDirectory, ".hoya/cluster/$CLUSTER"))

    
    
  }


}
