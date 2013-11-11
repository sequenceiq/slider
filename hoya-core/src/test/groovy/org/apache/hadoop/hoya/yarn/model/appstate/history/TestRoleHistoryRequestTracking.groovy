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

package org.apache.hadoop.hoya.yarn.model.appstate.history

import org.apache.hadoop.hoya.yarn.appmaster.state.NodeInstance
import org.apache.hadoop.hoya.yarn.appmaster.state.OutstandingRequest
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleHistory
import org.apache.hadoop.hoya.yarn.model.mock.BaseMockAppStateTest
import org.apache.hadoop.hoya.yarn.model.mock.MockFactory
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.junit.Before
import org.junit.Test

/**
 * Test the RH availability list and request tracking: that hosts
 * get removed and added 
 */
class TestRoleHistoryRequestTracking extends BaseMockAppStateTest {


  NodeInstance age1Active4 = nodeInstance(1, 4, 0, 0)
  NodeInstance age2Active2 = nodeInstance(2, 2, 0, 1)
  NodeInstance age3Active0 = nodeInstance(3, 0, 0, 0)
  NodeInstance age4Active1 = nodeInstance(4, 1, 0, 0)
  NodeInstance age2Active0 = nodeInstance(2, 0, 0, 0)
  NodeInstance empty = new NodeInstance("empty", MockFactory.ROLE_COUNT)

  List<NodeInstance> nodes = [age2Active2, age2Active0, age4Active1, age1Active4, age3Active0]
  RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
  Resource resource = Resource.newInstance(1, 1)

  @Override
  String getTestName() {
    return "TestRoleHistoryAvailableList"
  }
  
  @Before
  public void setupNodeMap() {
    roleHistory.insert(nodes)
    roleHistory.buildAvailableNodeLists();
  }

  @Test
  public void testAvailableListBuiltForRoles() throws Throwable {
    List<NodeInstance> available0 = roleHistory.cloneAvailableList(0)
    assertListEquals([age3Active0, age2Active0], available0)
  }
  
  @Test
  public void testRequestedNodeOffList() throws Throwable {
    List<NodeInstance> available0 = roleHistory.cloneAvailableList(0)
    NodeInstance ni = roleHistory.findNodeForNewInstance(0)
    assert age3Active0 == ni
    AMRMClient.ContainerRequest req = roleHistory.requestInstanceOnNode(ni,
                                                                        0,
                                                                        resource)
    List<NodeInstance> a2 = roleHistory.cloneAvailableList(0)
    assertListEquals([age2Active0], a2)
  }
  
  @Test
  public void testFindAndRequestNode() throws Throwable {
    AMRMClient.ContainerRequest req = roleHistory.requestNode(0, resource)

    assert age3Active0.hostname == req.nodes[0]
    List<NodeInstance> a2 = roleHistory.cloneAvailableList(0)
    assertListEquals([age2Active0], a2)
  }
  
  @Test
  public void testRequestedNodeIntoReqList() throws Throwable {
    AMRMClient.ContainerRequest req = roleHistory.requestNode(0, resource)
    List<OutstandingRequest> requests = roleHistory.outstandingRequestList
    assert requests.size() == 1
    assert age3Active0.hostname == requests[0].hostname
  }
  
}
