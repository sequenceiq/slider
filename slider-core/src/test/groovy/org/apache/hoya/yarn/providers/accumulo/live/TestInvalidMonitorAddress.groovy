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

package org.apache.hoya.yarn.providers.accumulo.live

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import org.apache.hoya.HoyaExitCodes
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.api.RoleKeys
import org.apache.hoya.providers.accumulo.AccumuloKeys
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.providers.accumulo.AccumuloTestBase
import org.apache.hadoop.yarn.service.launcher.ServiceLaunchException
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

@CompileStatic
@Slf4j

class TestInvalidMonitorAddress extends AccumuloTestBase {

  @Test
  public void testInvalidMonitorAddress() throws Throwable {
    String clustername = "test_invalid_monitor_address"
    createMiniCluster(clustername, getConfiguration(), 1, true)

    describe "verify that bad Java heap options are picked up"
    
    Map<String, Integer> roles = [
        (AccumuloKeys.ROLE_MASTER): 1,
        (AccumuloKeys.ROLE_TABLET): 1,
        (AccumuloKeys.ROLE_MONITOR): 1,
    ];
    try {
      ServiceLauncher launcher = createAccCluster(clustername, roles,
           [
               Arguments.ARG_COMP_OPT, AccumuloKeys.ROLE_MONITOR, RoleKeys.ROLE_ADDITIONAL_ARGS, "--address foobar",
           ],
           true,
           true)
      HoyaClient hoyaClient = (HoyaClient) launcher.service
      addToTeardown(hoyaClient);
      ClusterDescription status = hoyaClient.clusterDescription
      dumpClusterDescription("Remote CD", status)
    } catch (ServiceLaunchException e) {
      assertExceptionDetails(e, HoyaExitCodes.EXIT_YARN_SERVICE_FAILED)
    }
    
  }

}
