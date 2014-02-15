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

package org.apache.hoya.yarn.providers.agent

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hoya.api.RoleKeys
import org.apache.hoya.exceptions.BadConfigException
import org.apache.hoya.providers.agent.AgentKeys
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.client.HoyaClient
import org.junit.Test

@CompileStatic
@Slf4j
class TestBuildBasicAgent extends AgentTestBase {


  @Test
  public void testBuildMultipleRoles() throws Throwable {

    def clustername = "test_build_basic_agent"
    createMiniCluster(
        clustername,
        configuration,
        1,
        1,
        1,
        true,
        false)
    buildAgentCluster("test_build_basic_agent_node_only",
                      [(AgentKeys.ROLE_NODE): 5],
                      [],
                      true, false,
                      false)
    def master = "hbase-master"

    def rs = "hbase-rs"
    ServiceLauncher<HoyaClient>  launcher= buildAgentCluster(clustername,
                                                             [
                                                                 (AgentKeys.ROLE_NODE): 5,
                                                                 (master): 1,
                                                                 (rs): 5
                                                             ],
                                                             [Arguments.ARG_ROLEOPT, master, RoleKeys.ROLE_PRIORITY, "2",
                                                                 Arguments.ARG_ROLEOPT, rs, RoleKeys.ROLE_PRIORITY, "3"],
                                                             true, false,
                                                             false)
    def cd = launcher.service.loadPersistedClusterDescription(clustername)
    dumpClusterDescription("$clustername:",cd)
    cd.getMandatoryRoleOpt(AgentKeys.ROLE_NODE, RoleKeys.ROLE_PRIORITY)
    assert "2" == cd.getMandatoryRoleOpt(master, RoleKeys.ROLE_PRIORITY)
    assert "5" == cd.getMandatoryRoleOpt(rs, RoleKeys.ROLE_INSTANCES)
    
    // now create an instance with no role priority for the rs
    try {
      buildAgentCluster(clustername + "-2",
                        [
                            (AgentKeys.ROLE_NODE): 5,
                            (master): 1,
                            (rs): 5
                        ],
                        [Arguments.ARG_ROLEOPT, master, RoleKeys.ROLE_PRIORITY, "2",
                        ],
                        true, false,
                        false)
      fail("Expected an exception")
    } catch (BadConfigException expected) {
    } 
    try {
      buildAgentCluster(clustername + "-3",
                        [
                            (AgentKeys.ROLE_NODE): 5,
                            (master): 1,
                            (rs): 5
                        ],
                        [Arguments.ARG_ROLEOPT, master, RoleKeys.ROLE_PRIORITY, "2",
                            Arguments.ARG_ROLEOPT, rs, RoleKeys.ROLE_PRIORITY, "2"],

                        true, false,
                        false)
      fail("Expected an exception")
    } catch (BadConfigException expected) {
    }
  }

}
