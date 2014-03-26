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
import org.apache.hoya.yarn.client.HoyaClient
import org.junit.Test

import static org.apache.hoya.api.ResourceKeys.COMPONENT_PRIORITY
import static org.apache.hoya.providers.agent.AgentKeys.*
import static org.apache.hoya.yarn.Arguments.ARG_COMP_OPT
import static org.apache.hoya.yarn.Arguments.ARG_COMP_OPT_SHORT
import static org.apache.hoya.yarn.Arguments.ARG_OPTION
import static org.apache.hoya.yarn.Arguments.ARG_RES_COMP_OPT
import static org.apache.hoya.yarn.Arguments.ARG_RES_COMP_OPT_SHORT
import static org.apache.hoya.yarn.Arguments.ARG_ROLEOPT

/**
 * Tests an echo command
 */
@CompileStatic
@Slf4j
class TestAppScript extends AgentTestBase {

  @Override
  void checkTestAssumptions(YarnConfiguration conf) {

  }

  @Test
    public void testEchoOperation() throws Throwable {
        def clustername = "test_app_script"
        createMiniCluster(
                clustername,
                configuration,
                1,
                1,
                1,
                true,
                false)

        File hoya_core = new File(".").absoluteFile
        String master_script_py = "src/test/python/hbase_master.py"
        File master_script_py_path = new File(hoya_core, master_script_py)
        assert master_script_py_path.exists()
        String rs_script_py = "src/test/python/hbase_rs.py"
        File rs_script_py_path = new File(hoya_core, rs_script_py)
        assert rs_script_py_path.exists()

        def master_role = "master"
        def rs_role = "rs"
        Map<String, Integer> roles = [
                (master_role): 1,
                (rs_role): 1,
        ];
        ServiceLauncher<HoyaClient> launcher = buildAgentCluster(clustername,
                roles,
                [
                        ARG_OPTION, CONTROLLER_URL, "http://localhost",
                        ARG_OPTION, PACKAGE_PATH, hoya_core.absolutePath,

                        ARG_COMP_OPT, master_role, PACKAGE_PATH, hoya_core.absolutePath,
                        ARG_RES_COMP_OPT, master_role, COMPONENT_PRIORITY, "1",
                        ARG_COMP_OPT_SHORT, master_role, SCRIPT_PATH, master_script_py,
                        ARG_COMP_OPT, master_role, SERVICE_NAME, "HBASE",
                        ARG_COMP_OPT, master_role, APP_HOME, "/dev/null",

                        ARG_COMP_OPT, rs_role, PACKAGE_PATH, hoya_core.absolutePath,
                        ARG_RES_COMP_OPT, rs_role, COMPONENT_PRIORITY, "2",
                        ARG_COMP_OPT, rs_role, SCRIPT_PATH, rs_script_py,
                        ARG_COMP_OPT, rs_role, SERVICE_NAME, "HBASE",
                        ARG_COMP_OPT, rs_role, APP_HOME, "/dev/null",
                ],
                true, true,
                true)
        HoyaClient hoyaClient = launcher.service

        waitForRoleCount(hoyaClient, roles, AGENT_CLUSTER_STARTUP_TIME)
        //sleep a bit
        sleep(20000)
        //expect the role count to be the same
        waitForRoleCount(hoyaClient, roles, 1000)

    }
}
