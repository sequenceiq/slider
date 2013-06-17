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









package org.apache.hadoop.hoya.yarn.cluster.live

import groovy.util.logging.Commons
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.tools.Duration
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@Commons
class TestLiveRegionService extends YarnMiniClusterTestBase {


  @Test
  public void testLiveRegionService() throws Throwable {
    describe("create a cluster with only region service; spin" +
             " waiting for the RS node to come up")

    //launch fake master
    String clustername = "TestLiveRegionService"
    createMiniCluster(clustername, new YarnConfiguration(), 1, true)
    ServiceLauncher launcher = createHoyaCluster(clustername, 1, 
                                                 [
//                                                     CommonArgs.ARG_X_NO_MASTER
                                                 ],
                                                 true)
    //now look for the explicit sevice
    //do the low level operations to get a better view of what is going on 
    HoyaClient hoyaClient = (HoyaClient) launcher.service

    Duration duration = new Duration(HBASE_CLUSTER_STARTUP_TIME);
    duration.start()
    int workerCount = 0;
    while (workerCount == 0) {
      ClusterDescription status = hoyaClient.getClusterStatus(clustername)
      //log.info("Status $status")
      workerCount = status.regionNodes.size()
      if (workerCount == 0) {
        assert !duration.limitExceeded
        Thread.sleep(5000)
      }
    }
    //here there is >1 worker
    //sleep for a bit to let the RM do its thing
    Thread.sleep(10000)
    ClusterDescription status = hoyaClient.getClusterStatus(clustername)
    log.info(status)

  }


}
