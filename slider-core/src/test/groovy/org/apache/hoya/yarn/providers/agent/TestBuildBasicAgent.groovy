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
import org.apache.hoya.HoyaKeys
import org.apache.hoya.api.ResourceKeys
import org.apache.hoya.api.RoleKeys
import org.apache.hoya.core.conf.AggregateConf
import org.apache.hoya.core.persist.ConfPersister
import org.apache.hoya.exceptions.BadConfigException
import org.apache.hoya.providers.agent.AgentKeys
import org.apache.hoya.yarn.client.HoyaClient
import org.junit.Test

import static org.apache.hoya.providers.agent.AgentKeys.*
import static org.apache.hoya.yarn.Arguments.*

@CompileStatic
@Slf4j
class TestBuildBasicAgent extends AgentTestBase {
  static String TEST_FILES = "./src/test/resources/org/apache/hoya/providers/agent/tests/"

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
    assert "5" == rscomponent.getMandatoryOption(ResourceKeys.COMPONENT_INSTANCES)

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
    
    //duplicate priorities
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



    def cluster4 = clustername + "-4"

    def jvmopts = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
    buildAgentCluster(cluster4,
        [
            (master)             : 1,
            (rs)                 : 5
        ],
        [
            ARG_COMP_OPT, HoyaKeys.COMPONENT_AM, RoleKeys.JVM_OPTS,
            jvmopts
        ],

        true, false,
        false)

    //now we want to look at the value
    AggregateConf instanceDefinition = loadInstanceDefinition(cluster4)
    def opt = instanceDefinition.getAppConfOperations().getComponentOpt(
        HoyaKeys.COMPONENT_AM,
        RoleKeys.JVM_OPTS,
        "")

    assert jvmopts == opt
  }

  public AggregateConf loadInstanceDefinition(String name) {
    def cluster4
    def hoyaFS = createHoyaFileSystem()
    def dirPath = hoyaFS.buildHoyaClusterDirPath(name)
    ConfPersister persister = new ConfPersister(hoyaFS, dirPath)
    AggregateConf instanceDefinition = new AggregateConf();
    persister.load(instanceDefinition)
    return instanceDefinition
  }

  @Test
  public void testTemplateArgs() throws Throwable {


    def clustername = "test_build_template_args"
    createMiniCluster(
        clustername,
        configuration,
        1,
        1,
        1,
        true,
        false)
    buildAgentCluster("test_build_template_args_good",
        [:],
        [

            ARG_OPTION, CONTROLLER_URL, "http://localhost",
            ARG_PACKAGE, ".",
            ARG_RESOURCES, TEST_FILES + "good/resources.json",
            ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
        ],
        true, false,
        false)
  }
  
  
  @Test
  public void testBadTemplates() throws Throwable {


    def clustername = "test_bad_template_args"
    createMiniCluster(
        clustername,
        configuration,
        1,
        1,
        1,
        true,
        false)
    
    try {
      
      //initial: get the two mixed up

      def badArgs1 = "test_build_template_args_bad-1"
      buildAgentCluster(badArgs1,
          [:],
          [

              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_PACKAGE, ".",
              ARG_TEMPLATE, TEST_FILES + "good/resources.json",
              ARG_RESOURCES, TEST_FILES + "good/appconf.json"
          ],
          true, false,
          false)
      AggregateConf instanceDefinition = loadInstanceDefinition(badArgs1)
      log.error("configuration was not rejected ", instanceDefinition)
      fail("Expected an exception from an incomplete instance definition")
    } catch (BadConfigException expected) {
    }

    try {
      
      //initial: get the two mixed up
      buildAgentCluster("test_build_template_args_bad-2",
          [:],
          [

              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_PACKAGE, ".",
              ARG_TEMPLATE, TEST_FILES + "bad/appconf-1.json",
          ],
          true, false,
          false)

      fail("Expected an exception from a bad app conf")
    } catch (BadConfigException expected) {
    }
    
    try {
      
      //initial: get the two mixed up
      buildAgentCluster("test_build_template_args_bad-3",
          [:],
          [

              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_PACKAGE, ".",
              ARG_TEMPLATE, "unknown.json",
          ],
          true, false,
          false)

      fail("Expected a file not found exception")
    } catch (BadConfigException expected) {
    }


    try {

      //initial: get the two mixed up
      buildAgentCluster("test_build_template_args_bad-4",
          [:],
          [

              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_PACKAGE, ".",
              ARG_TEMPLATE, TEST_FILES + "bad/appconf-2.json",
          ],
          true, false,
          false)

      fail("Expected an exception from a bad app conf")
    } catch (BadConfigException expected) {
    }

  }
  
  
}
