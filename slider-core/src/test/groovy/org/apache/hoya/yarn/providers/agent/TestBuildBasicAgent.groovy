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
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hoya.api.ResourceKeys
import org.apache.hoya.exceptions.BadConfigException
import org.apache.hoya.providers.agent.AgentKeys
import org.apache.hoya.yarn.client.HoyaClient
import org.junit.Test

import static org.apache.hoya.providers.agent.AgentKeys.*
import static org.apache.hoya.yarn.Arguments.*

@CompileStatic
@Slf4j
class TestBuildBasicAgent extends AgentTestBase {


  @Override
  void checkTestAssumptions(YarnConfiguration conf) {

  }

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
        [
            ARG_OPTION, CONTROLLER_URL, "http://localhost",
            ARG_PACKAGE, ".",
            ARG_OPTION, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_COMP_OPT, AgentKeys.ROLE_NODE, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_RES_COMP_OPT, AgentKeys.ROLE_NODE, ResourceKeys.COMPONENT_PRIORITY, "1",
        ],
        true, false,
        false)

    def master = "hbase-master"
    def rs = "hbase-rs"
    ServiceLauncher<HoyaClient> launcher = buildAgentCluster(clustername,
        [
            (AgentKeys.ROLE_NODE): 5,
            (master)             : 1,
            (rs)                 : 5
        ],
        [
            ARG_OPTION, CONTROLLER_URL, "http://localhost",
            ARG_OPTION, PACKAGE_PATH, ".",
            ARG_COMP_OPT, master, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_COMP_OPT, rs, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_RES_COMP_OPT, master, ResourceKeys.COMPONENT_PRIORITY, "2",
            ARG_RES_COMP_OPT, rs, ResourceKeys.COMPONENT_PRIORITY, "3",
            ARG_COMP_OPT, master, AgentKeys.SERVICE_NAME, "HBASE",
            ARG_COMP_OPT, rs, AgentKeys.SERVICE_NAME, "HBASE",
            ARG_COMP_OPT, master, AgentKeys.APP_HOME, "/share/hbase/hbase-0.96.1-hadoop2",
            ARG_COMP_OPT, rs, AgentKeys.APP_HOME, "/share/hbase/hbase-0.96.1-hadoop2",
            ARG_COMP_OPT, AgentKeys.ROLE_NODE, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_RES_COMP_OPT, AgentKeys.ROLE_NODE, ResourceKeys.COMPONENT_PRIORITY, "1",
        ],
        true, false,
        false)
    def instanceD = launcher.service.loadPersistedClusterDescription(
        clustername)
    dumpClusterDescription("$clustername:", instanceD)
    def resource = instanceD.getResourceOperations()


    def agentComponent = resource.getMandatoryComponent(AgentKeys.ROLE_NODE)
    agentComponent.getMandatoryOption(ResourceKeys.COMPONENT_PRIORITY)

    def masterC = resource.getMandatoryComponent(master)
    assert "2" == masterC.getMandatoryOption(ResourceKeys.COMPONENT_PRIORITY)

    def rscomponent = resource.getMandatoryComponent(rs)
    assert "5" ==
           rscomponent.getMandatoryOption(ResourceKeys.COMPONENT_INSTANCES)

    // now create an instance with no role priority for the rs
    try {
      buildAgentCluster(clustername + "-2",
          [
              (AgentKeys.ROLE_NODE): 5,
              (master)             : 1,
              (rs)                 : 5
          ],
          [
              ARG_RES_COMP_OPT, master, ResourceKeys.COMPONENT_PRIORITY, "2",
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
              (master)             : 1,
              (rs)                 : 5
          ],
          [
              ARG_RES_COMP_OPT, master, ResourceKeys.COMPONENT_PRIORITY, "2",
              ARG_RES_COMP_OPT, rs, ResourceKeys.COMPONENT_PRIORITY, "2"],

          true, false,
          false)
      fail("Expected an exception")
    } catch (BadConfigException expected) {
    }
  }

}
