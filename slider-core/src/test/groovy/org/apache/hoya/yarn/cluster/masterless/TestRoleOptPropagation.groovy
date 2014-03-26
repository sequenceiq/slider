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

package org.apache.hoya.yarn.cluster.masterless

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hoya.HoyaKeys
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.exceptions.BadCommandArgumentsException
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

@CompileStatic
@Slf4j

class TestRoleOptPropagation extends HBaseMiniClusterTestBase {

  @Test
  
  public void testRoleOptPropagation() throws Throwable {
    skip("Disabled")
    String clustername = "test_role_opt_propagation"
    createMiniCluster(clustername, getConfiguration(), 1, true)

    describe "verify that role options propagate down to deployed roles"

    String ENV = "env.ENV_VAR"
    ServiceLauncher launcher = createHoyaCluster(clustername,
                                                 [
                                                     (HBaseKeys.ROLE_MASTER): 0,
                                                     (HBaseKeys.ROLE_WORKER): 0,
                                                 ],
                                                 [
                                                     Arguments.ARG_COMP_OPT, HBaseKeys.ROLE_MASTER, ENV, "4",
                                                 ],
                                                 true,
                                                 true,
                                                 [:])
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);
    ClusterDescription status = hoyaClient.clusterDescription
    Map<String, String> masterRole = status.getRole(HBaseKeys.ROLE_MASTER);
    dumpClusterDescription("Remote CD", status)
    assert masterRole[ENV] == "4"

  }

  @Test
  public void testUnknownRole() throws Throwable {
    String clustername = "test_unknown_role"
    createMiniCluster(clustername, getConfiguration(), 1, true)

    describe "verify that unknown role results in cluster creation failure"
    try {
      String MALLOC_ARENA = "env.MALLOC_ARENA_MAX"
      ServiceLauncher launcher = createHoyaCluster(clustername,
                                                 [
                                                     (HBaseKeys.ROLE_MASTER): 0,
                                                     (HBaseKeys.ROLE_WORKER): 0,
                                                 ],
                                                 [
                                                     Arguments.ARG_COMP_OPT, HoyaKeys.ROLE_HOYA_AM, MALLOC_ARENA, "4",
                                                     Arguments.ARG_COMP_OPT, "unknown", MALLOC_ARENA, "3",
                                                 ],
                                                 true,
                                                 true,
                                                 [:])
      assert false
    } catch (BadCommandArgumentsException bcae) {
      /* expected */
    }
  }
}
