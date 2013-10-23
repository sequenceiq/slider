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

package org.apache.hadoop.hoya.yarn.cluster.masterless

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.hoya.HoyaKeys
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.providers.hbase.HBaseKeys
import org.apache.hadoop.hoya.yarn.Arguments
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

@CompileStatic
@Slf4j

class TestRoleOptPropagation extends HBaseMiniClusterTestBase {

  @Test
  public void testRoleOptPropagation() throws Throwable {
    String clustername = "TestRoleOptPropagation"
    createMiniCluster(clustername, createConfiguration(), 1, true)

    describe "verify that role options propagate down to deployed roles"

    String MALLOC_ARENA = "env.MALLOC_ARENA_MAX"
    ServiceLauncher launcher = createHoyaCluster(clustername,
                                                 [
                                                     (HBaseKeys.ROLE_MASTER): 0,
                                                     (HBaseKeys.ROLE_WORKER): 0,
                                                 ],
                                                 [
                                                     Arguments.ARG_ROLEOPT, HoyaKeys.ROLE_HOYA_AM, MALLOC_ARENA, "4",
                                                     Arguments.ARG_ROLEOPT, "unknown", MALLOC_ARENA, "3",
                                                 ],
                                                 true,
                                                 true, [:])
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);
    ClusterDescription status = hoyaClient.clusterDescription
    Map<String, String> masterRole = status.getRole(HoyaKeys.ROLE_HOYA_AM);
    assert masterRole[MALLOC_ARENA] == "4"

    Map<String, String> unknownRole = status.getRole("unknown");
    assert unknownRole[MALLOC_ARENA] == "3"

    dumpClusterDescription("Remote CD", status)
  }

}
