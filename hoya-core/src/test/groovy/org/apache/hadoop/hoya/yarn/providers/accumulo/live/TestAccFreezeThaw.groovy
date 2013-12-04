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

package org.apache.hadoop.hoya.yarn.providers.accumulo.live

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.hoya.providers.accumulo.AccumuloConfigFileOptions
import org.apache.hadoop.hoya.providers.accumulo.AccumuloKeys
import org.apache.hadoop.hoya.tools.ZKIntegration
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.providers.accumulo.AccumuloTestBase
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

@CompileStatic
@Slf4j
class TestAccFreezeThaw extends AccumuloTestBase {

  @Test
  public void testAccFreezeThaw() throws Throwable {
    String clustername = "test_acc_freeze_thaw"
    int tablets = 1
    int monitor = 1
    int gc = 1
    createMiniCluster(clustername, createConfiguration(), 1, 1, 1, true, false)
    describe(" Create an accumulo cluster");

    //make sure that ZK is up and running at the binding string
    ZKIntegration zki = createZKIntegrationInstance(ZKBinding, clustername, false, false, 5000)
    log.info("ZK up at $zki");
    //now launch the cluster
    Map<String, Integer> roles = [
        (AccumuloKeys.ROLE_MASTER): 1,
        (AccumuloKeys.ROLE_TABLET): tablets,
        (AccumuloKeys.ROLE_MONITOR): monitor,
        (AccumuloKeys.ROLE_GARBAGE_COLLECTOR): gc
    ];
    ServiceLauncher launcher = createAccCluster(clustername, roles, [], true, true)
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);

    
    waitForRoleCount(hoyaClient, roles, ACCUMULO_CLUSTER_STARTUP_TO_LIVE_TIME)
    //now give the cluster a bit of time to actually start work

    log.info("Sleeping for a while")
    sleep(ACCUMULO_GO_LIVE_TIME);
    //verify that all is still there
    waitForRoleCount(hoyaClient, roles, 0, "extended cluster operation")

    String page = fetchLocalPage(AccumuloConfigFileOptions.MONITOR_PORT_CLIENT_INT,
                                 AccumuloKeys.MONITOR_PAGE_JSON)
    log.info(page);

    log.info("Freezing")
    clusterActionFreeze(hoyaClient, clustername, "freeze");
    waitForAppToFinish(hoyaClient)
    
    //make sure the fetch fails
    try {
      page = fetchLocalPage(AccumuloConfigFileOptions.MONITOR_PORT_CLIENT_INT,
                     AccumuloKeys.MONITOR_PAGE_JSON)
      //this should have failed
      log.error("The accumulo monitor is still live")
      log.error(page)
    } catch (ConnectException expected) {
      //
      log.debug("expected exception", expected)
    }
    //force kill any accumulo processes
    killAllAccumuloProcesses()
    sleep(ACCUMULO_GO_LIVE_TIME);

    log.info("Thawing")
    
    ServiceLauncher launcher2 = thawHoyaCluster(clustername, [], true);
    HoyaClient hoyaClient2 = (HoyaClient) launcher2.service
    addToTeardown(hoyaClient2)
    waitForRoleCount(hoyaClient, roles, ACCUMULO_CLUSTER_STARTUP_TO_LIVE_TIME, "thawing")


    sleep(ACCUMULO_GO_LIVE_TIME);
    //verify that all is still there
    waitForRoleCount(hoyaClient, roles, 0, "extended cluster operation after thawing")
    page = fetchLocalPage(
        AccumuloConfigFileOptions.MONITOR_PORT_CLIENT_INT,
        AccumuloKeys.MONITOR_PAGE_JSON)
    log.info(page);
  }

}
