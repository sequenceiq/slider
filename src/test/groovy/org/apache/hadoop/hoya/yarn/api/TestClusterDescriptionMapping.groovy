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

package org.apache.hadoop.hoya.yarn.api

import groovy.transform.CompileStatic
import groovy.util.logging.Commons
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.api.ClusterNode
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.junit.Test

/**
 test CD serialization/deserialization to and from JSON
 */
@CompileStatic
@Commons
class TestClusterDescriptionMapping extends YarnMiniClusterTestBase {


  ClusterDescription createCD() {
    ClusterDescription cd = new ClusterDescription();
    cd.name = "test"
    cd.state = ClusterDescription.STATE_LIVE;
    cd.masters = 1;
    ClusterNode node = new ClusterNode("masternode")
    node.state = ClusterDescription.STATE_LIVE
    node.output = ["line1","line2"]
    cd.masterNodes = [node]
    
    cd.startTime = System.currentTimeMillis()

    return cd;
  }

  ClusterDescription parse(String s) {
    try {
      return ClusterDescription.fromJson(s);
    } catch (IOException e) {
      log.info("exception parsing: \n" + s);
      throw e;
    }
  }

  ClusterDescription roundTrip(ClusterDescription src) {
    return parse(src.toJsonString());
  }

  @Test
  public void testJsonify() throws Throwable {
    log.info(createCD().toJsonString())
  }

  @Test
  public void testEmptyRoundTrip() throws Throwable {
    roundTrip(new ClusterDescription())
  }

  @Test
  public void testRTrip() throws Throwable {
    ClusterDescription original = createCD()
    ClusterDescription received = roundTrip(original)
    assert received.masterNodes.size()>0
    ClusterNode node = received.masterNodes[0]
    assert node.output.length == 2;
  }
}
