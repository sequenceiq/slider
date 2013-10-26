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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.hoya.yarn.appmaster.state.AbstractRMOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.ContainerRequestOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.RMOperationHandler
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleInstance
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleStatus
import org.apache.hadoop.hoya.yarn.model.mock.BaseMockAppStateTest
import org.apache.hadoop.hoya.yarn.model.mock.MockRMOperationHandler
import org.apache.hadoop.hoya.yarn.model.mock.MockRoles
import org.junit.Test

@CompileStatic
@Slf4j
class TestContainerAddRemove extends BaseMockAppStateTest implements MockRoles {
  
  @Test
  public void testContainerAddRemoveOps() throws Throwable {


    
    role1Status.desired = 1
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 1
    ContainerRequestOperation operation = (ContainerRequestOperation)ops[0]
    assert operation.request.priority.priority == 1
    RMOperationHandler handler = new MockRMOperationHandler()
    handler.execute(ops)
    
    //tell the container its been allocated
    
  }    
    

  public RoleStatus getRole1Status() {
    return appState.lookupRoleStatus(ROLE1)
  }

}
