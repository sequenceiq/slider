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

import static org.apache.hoya.api.RoleKeys.*
import static org.apache.hoya.providers.agent.AgentKeys.*
import static org.apache.hoya.yarn.Arguments.*
import org.apache.hoya.yarn.client.HoyaClient
import org.junit.Test

/**
 * Tests an echo command
 */
@CompileStatic
@Slf4j
class TestAgentEcho extends AgentTestBase {

  @Test
  public void testEchoOperation() throws Throwable {
    def clustername = "test_agent_echo"
    createMiniCluster(
        clustername,
        configuration,
        1,
        1,
        1,
        true,
        false)

    File hoya_core = new File(".").absoluteFile
    String echo_py = "src/test/python/echo.py"
    File echo_py_path = new File(hoya_core, echo_py)
    assert echo_py_path.exists()

    def role = "echo"
    Map<String, Integer> roles = [
        (role): 1,
    ];
    ServiceLauncher<HoyaClient> launcher = buildAgentCluster(clustername,
        roles,
        [
            ARG_OPTION, CONTROLLER_URL, "http://localhost",
            ARG_OPTION, PACKAGE_PATH, hoya_core.absolutePath,

            ARG_ROLEOPT, role, ROLE_PRIORITY, "2",
            ARG_ROLEOPT, role, AGENT_SCRIPT, echo_py,
        ],
        true, true,
        true)
    HoyaClient hoyaClient = launcher.service

    waitForRoleCount(hoyaClient, roles, AGENT_CLUSTER_STARTUP_TIME)

  }
}
