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

package org.apache.hadoop.hoya.yarn.model.appstate

import org.apache.hadoop.hoya.yarn.appmaster.state.RoleInstance
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleStatus
import org.apache.hadoop.hoya.yarn.model.mock.BaseMockAppStateTest
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.junit.Test

class TestContainerAddRemove extends BaseMockAppStateTest {
  
  @Test
  public void testContainerAddRemoveOps() throws Throwable {


    Container container = factory.newContainer()
    RoleInstance roleInstance = new RoleInstance(container)
    appState.addLaunchedContainer(container,
                                  roleInstance)
    List<RoleInstance> active = appState.cloneActiveContainerList()
    assert active.size() == 1

  }


  @Test
  public void testBuildRequestForRole() throws Throwable {
    AMRMClient.ContainerRequest request =
        appState.buildContainerResourceAndRequest(role1Status,
                                                  factory.newResource());
  }

  public RoleStatus getRole1Status() {
    return appState.lookupRoleStatus(factory.ROLE1)
  }

}
