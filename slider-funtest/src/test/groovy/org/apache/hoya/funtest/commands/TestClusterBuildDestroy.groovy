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

package org.apache.hoya.funtest.commands

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.service.launcher.LauncherExitCodes
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.HoyaActions
import org.apache.hoya.funtest.framework.CommandTestBase
import org.apache.hoya.funtest.framework.FuntestProperties
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

@CompileStatic
@Slf4j
public class TestClusterBuildDestroy extends CommandTestBase
    implements FuntestProperties, Arguments {


  static String CLUSTER = "test_cluster_build_destroy"
  

  @BeforeClass
  public static void prepareCluster() {
    assumeFunctionalTestsEnabled();
    setupCluster(CLUSTER)
  }

  @AfterClass
  public static void destroyCluster() {
    teardown(CLUSTER)
  }
  
  @Test
  public void testBuildAndDestroyCluster() throws Throwable {
    '''
  bin/hoya build cl1 \\
    --zkhosts sandbox \\
     \\
    --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \\
    --appconf file://./src/test/configs/sandbox/hbase \\
    --roleopt master app.infoport 8180  \\
    --role master 1 
'''
    hoya(0,
        [
            HoyaActions.ACTION_BUILD,
            CLUSTER,
            ARG_ZKHOSTS,
            HOYA_CONFIG.get(KEY_HOYA_TEST_ZK_HOSTS, DEFAULT_HOYA_ZK_HOSTS),
            ARG_IMAGE,
            HOYA_CONFIG.get(KEY_HOYA_TEST_HBASE_TAR),
            ARG_CONFDIR,
            HOYA_CONFIG.get(KEY_HOYA_TEST_HBASE_APPCONF),
            ARG_ROLE, HBaseKeys.ROLE_MASTER, "1",
            ARG_ROLE, HBaseKeys.ROLE_WORKER, "1",
            ARG_ROLEOPT, HBaseKeys.ROLE_MASTER, "app.infoport", "8180",
        ])

    assert clusterFS.exists(new Path(clusterFS.homeDirectory, ".hoya/cluster/$CLUSTER"))
    //cluster exists if you don't want it to be live
    exists(0, CLUSTER, false)
    // condition returns false if it is required to be live
    exists(LauncherExitCodes.EXIT_FALSE, CLUSTER, true)
    destroy(CLUSTER)

  }


}
