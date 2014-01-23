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
import org.apache.hadoop.hbase.IntegrationTestsDriver

class TestHBaseIntegration extends TestFunctionalHBaseCluster {

  @Override
  String getClusterName() {
    return "test_hbase_integration"
  }

  @Override
  void clusterLoadOperations(
      Configuration clientConf,
      int numWorkers,
      Map<String, Integer> roleMap) {
    int numKeys = 4000 * numWorkers
    String[] args = ["-r", "IntegrationTestManyRegions", "-zk",
       clientConf.get("hbase.zookeeper.quorum"), "-zk_root", "/yarnapps_hoya_yarn_"+getClusterName()]
    IntegrationTestsDriver driver = new IntegrationTestsDriver();
    driver.setConf(clientConf)
    int ret = driver.run(args);
    assert ret == 0;
  }


  public int getWorkerPortAssignment() {
    return 0
  }

  public int getMasterPortAssignment() {
    return 0
  }
}
