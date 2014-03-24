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

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.WebResource
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hoya.yarn.appmaster.web.rest.agent.RegistrationResponse
import org.apache.hoya.yarn.appmaster.web.rest.agent.RegistrationStatus
import org.apache.hoya.yarn.client.HoyaClient
import org.junit.Test

import javax.ws.rs.core.MediaType

import static org.apache.hoya.api.RoleKeys.*
import static org.apache.hoya.providers.agent.AgentKeys.*
import static org.apache.hoya.yarn.Arguments.*
import static org.apache.hoya.testtools.HoyaTestUtils.*;
import static org.apache.hoya.yarn.providers.agent.AgentTestUtils.*;

@CompileStatic
@Slf4j
class TestAgentAMManagementWS extends AgentTestBase {

  public static final String MANAGEMENT_URI = "hoyaam/ws/v1/slider/mgmt/";
  public static final String AGENT_URI = "hoyaam/ws/v1/slider/agent/";

  @Test
  public void testAgentAMManagementWS() throws Throwable {
    def clustername = "test_agentammanagementws"
    createMiniCluster(
        clustername,
        configuration,
        1,
        1,
        1,
        true,
        false)
    Map<String, Integer> roles = [:]
    File hoya_core = new File(".").absoluteFile
    ServiceLauncher<HoyaClient> launcher = buildAgentCluster(clustername,
        roles,
        [
            ARG_OPTION, CONTROLLER_URL, "http://localhost",
            ARG_OPTION, PACKAGE_PATH, hoya_core.absolutePath,
        ],
        true, true,
        true)
    HoyaClient hoyaClient = launcher.service
    def report = waitForClusterLive(hoyaClient)
    def trackingUrl = report.trackingUrl
    log.info("tracking URL is $trackingUrl")
    def agent_url = trackingUrl + AGENT_URI

    log.info("Agent URL is $agent_url")
    
    dumpClusterStatus(hoyaClient, "agent AM")

    String page = fetchWebPageWithoutError(agent_url + "register");
    log.info(page);
    
    //WS get
    Client client = createTestClient();


    WebResource webResource = client.resource(agent_url + "register/test");
    RegistrationResponse response = webResource.type(MediaType.APPLICATION_JSON)
                          .post(
        RegistrationResponse.class,
        createDummyJSONRegister());
    assert RegistrationStatus.OK == response.getResponseStatus();
    
  }
}
