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

package org.apache.hoya.funtest.accumulo

import org.apache.hadoop.yarn.conf.YarnConfiguration
import static org.apache.hoya.api.RoleKeys.*
import org.apache.hoya.funtest.framework.HoyaCommandTestBase
import org.apache.hoya.funtest.framework.HoyaShell
import org.junit.Before

import static org.apache.hoya.funtest.framework.HoyaFuntestProperties.KEY_HOYA_TEST_ACCUMULO_ENABLED
import static org.apache.hoya.providers.accumulo.AccumuloKeys.*
import static org.apache.hoya.yarn.Arguments.ARG_PROVIDER
import static org.apache.hoya.yarn.Arguments.ARG_ROLEOPT
import static org.apache.hoya.HoyaXMLConfKeysForTesting.*


/**
 * Anything specific to accumulo tests
 */
abstract class AccumuloCommandTestBase extends HoyaCommandTestBase {


  @Before
  public void verifyPreconditions() {
    assumeBoolOption(HOYA_CONFIG, KEY_HOYA_TEST_ACCUMULO_ENABLED, true)
    assumeStringOptionSet(HOYA_CONFIG, OPTION_ZK_HOME )
    assumeStringOptionSet(HOYA_CONFIG, OPTION_HADOOP_HOME)
  }

  /**
   * Create an accumulo cluster
   *
   * @param clustername
   * @param roles
   * @param extraArgs
   * @param blockUntilRunning
   * @param containerMemory
   * @return
   */
  public HoyaShell createAccCluster(String clustername,

                                    Map<String, Integer> roles,
                                    List<String> extraArgs,
                                    boolean blockUntilRunning,
                                    String containerMemory) {
    extraArgs << ARG_PROVIDER << PROVIDER_ACCUMULO;


    YarnConfiguration conf = HOYA_CONFIG
    def clusterOps = [
        (OPTION_ZK_HOME)    : conf.getTrimmed(
            OPTION_ZK_HOME),
        (OPTION_HADOOP_HOME):
            conf.getTrimmed(OPTION_HADOOP_HOME),
    ]

    extraArgs << ARG_ROLEOPT << ROLE_MASTER <<

    extraArgs << ARG_ROLEOPT << ROLE_MASTER <<
    YARN_MEMORY << containerMemory
    extraArgs << ARG_ROLEOPT << ROLE_TABLET <<
    YARN_MEMORY << containerMemory
    extraArgs << ARG_ROLEOPT << ROLE_MONITOR <<
    YARN_MEMORY << containerMemory
    extraArgs << ARG_ROLEOPT << ROLE_GARBAGE_COLLECTOR <<
    YARN_MEMORY << containerMemory

    return createHoyaCluster(clustername,
                             roles,
                             extraArgs,
                             blockUntilRunning,
                             clusterOps)
  }
}
