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

package org.apache.hoya.funtest.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.api.RoleKeys
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.params.ActionKillContainerArgs

import static org.apache.hoya.testtools.HBaseTestUtils.waitForHBaseRegionServerCount


class TestHBaseNodeFailure extends TestFunctionalHBaseCluster {
  
  @Override
  String getClusterName() {
    return "test_hbase_node_failure"
  }

  @Override
  String getDescription() {
    "Fail containers and verify that the cluster recovers"
  }

  @Override
  void clusterLoadOperations(Configuration clientConf, int numWorkers) {
    HoyaClient hoyaClient = bondToCluster(HOYA_CONFIG, clusterName)
    ClusterDescription c


    killInstanceOfRole(hoyaClient, HBaseKeys.ROLE_WORKER)
    // let it take
    sleep(5000)
    // then expect a restart
    waitForHBaseRegionServerCount(
        hoyaClient,
        clusterName,
        numWorkers,
        HBASE_LAUNCH_WAIT_TIME)

    def cd = hoyaClient.getClusterDescription()
    assert cd.roles[HBaseKeys.ROLE_WORKER][RoleKeys.ROLE_FAILED_INSTANCES] == "2"

  }

  /**
   * Kill a random in instance of a role in the cluster
   * @param hoyaClient client
   * @param role
   * @return ID of container killed
   */
  public String killInstanceOfRole(
      HoyaClient hoyaClient, String role) {
    ClusterDescription cd = hoyaClient.getClusterDescription()
    def instances = cd.instances[role]
    int size = instances.size()
    if (size) {
      String id = instances[new Random().nextInt(size)]
      ActionKillContainerArgs args = new ActionKillContainerArgs()
      args.id = id
      hoyaClient.actionKillContainer(clusterName, args)
      return id;
    } else {
      return null;
    }
  }
}
