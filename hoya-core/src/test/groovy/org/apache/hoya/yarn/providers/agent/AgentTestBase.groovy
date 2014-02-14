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
import org.apache.hoya.api.RoleKeys
import org.apache.hoya.yarn.HoyaActions
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.cluster.YarnMiniClusterTestBase

import static org.apache.hoya.HoyaXMLConfKeysForTesting.*
import static org.apache.hoya.providers.agent.AgentKeys.*
import static org.apache.hoya.testtools.HoyaTestUtils.assumeBoolOption
import static org.apache.hoya.yarn.Arguments.ARG_PROVIDER
import static org.apache.hoya.yarn.Arguments.ARG_ROLEOPT

/**
 * test base for all agent clusters
 */
@CompileStatic
@Slf4j
public abstract class AgentTestBase extends YarnMiniClusterTestBase {

  public static
  final int AGENT_CLUSTER_STARTUP_TIME = DEFAULT_HOYA_AGENT_LAUNCH_TIME

  /**
   * The time to sleep before trying to talk to the HBase Master and
   * expect meaningful results.
   */
  public static
  final int AGENT_CLUSTER_STARTUP_TO_LIVE_TIME = AGENT_CLUSTER_STARTUP_TIME
  public static final int AGENT_GO_LIVE_TIME = 60000


  @Override
  public String getTestConfigurationPath() {
    return "src/main/resources/" + CONF_RESOURCE;
  }

  @Override
  void setup() {
    super.setup()
//    assumeBoolOption(HOYA_CONFIG, KEY_HOYA_TEST_AGENT_ENABLED, true)
//    assumeArchiveDefined();
//    assumeApplicationHome();
    YarnConfiguration conf = testConfiguration
    assumeOtherSettings(conf)
  }


  @Override
  public String getArchiveKey() {
    return KEY_HOYA_TEST_AGENT_TAR
  }

  /**
   * Get the key for the application
   * @return
   */
  @Override
  public String getApplicationHomeKey() {
    return KEY_HOYA_TEST_AGENT_HOME
  }

  /**
   * Assume that HBase home is defined. This does not check that the
   * path is valid -that is expected to be a failure on tests that require
   * HBase home to be set.
   */

  public void assumeOtherSettings(YarnConfiguration conf) {

  }

  /**
   * Create an agent cluster
   * @param clustername
   * @param roles
   * @param extraArgs
   * @param deleteExistingData
   * @param blockUntilRunning
   * @return the cluster launcher
   */
  public ServiceLauncher<HoyaClient> buildAgentCluster(
      String clustername,
      Map<String, Integer> roles,
      List<String> extraArgs,
      boolean deleteExistingData,
      boolean blockUntilRunning) {
    extraArgs << ARG_PROVIDER << PROVIDER_AGENT;

    YarnConfiguration conf = testConfiguration

    def clusterOps = [
        :
    ]

    extraArgs << ARG_ROLEOPT << ROLE_NODE <<
    RoleKeys.YARN_MEMORY << YRAM

    return createOrBuildHoyaCluster(
        HoyaActions.ACTION_BUILD,
        clustername,
        roles,
        extraArgs,
        deleteExistingData,
        blockUntilRunning,
        clusterOps)
  }

  public String getApplicationHome() {
    return "/"
  }
}
