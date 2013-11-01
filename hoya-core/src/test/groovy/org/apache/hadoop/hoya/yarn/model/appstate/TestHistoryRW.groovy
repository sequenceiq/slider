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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hoya.avro.NodeAddress
import org.apache.hadoop.hoya.avro.RoleHistoryWriter
import org.apache.hadoop.hoya.yarn.appmaster.state.NodeInstance
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleHistory
import org.apache.hadoop.hoya.yarn.model.mock.BaseMockAppStateTest
import org.apache.hadoop.hoya.yarn.model.mock.MockFactory
import org.junit.Test

class TestHistoryRW extends BaseMockAppStateTest {

  static long time = 0;
  
  @Override
  String getTestName() {
    return "TestHistoryRW"
  }

  @Test
  public void testWriteReadEmpty() throws Throwable {
    RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
    roleHistory.onStart(fs, historyPath)
    
    Path history = roleHistory.saveHistory(time++)
    assert fs.isFile(history)
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    historyWriter.read(fs, history, roleHistory)
  }
  
  @Test
  public void testWriteReadData() throws Throwable {
    RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
    roleHistory.onStart(fs, historyPath)
    NodeAddress addr = new NodeAddress("localhost",80)
    NodeInstance instance = roleHistory.getOrCreateNodeInstance(addr)
    instance.getOrCreate(ROLE0)

    Path history = roleHistory.saveHistory(time++)
    assert fs.isFile(history)
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    historyWriter.read(fs, history, roleHistory)
  }
  
}
