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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hoya.avro.RoleHistoryWriter
import org.apache.hadoop.hoya.yarn.appmaster.state.NodeEntry
import org.apache.hadoop.hoya.yarn.appmaster.state.NodeInstance
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleHistory
import org.apache.hadoop.hoya.yarn.model.mock.BaseMockAppStateTest
import org.apache.hadoop.hoya.yarn.model.mock.MockFactory
import org.junit.Test

@Slf4j
@CompileStatic
class TestHistoryRW extends BaseMockAppStateTest {

  static long time = System.currentTimeMillis();
  
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
    String addr = "localhost"
    NodeInstance instance = roleHistory.getOrCreateNodeInstance(addr)
    NodeEntry ne1 = instance.getOrCreate(0)
    ne1.lastUsed = 0xf00d

    Path history = roleHistory.saveHistory(time++)
    assert fs.isFile(history)
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    RoleHistory rh2 = new RoleHistory(MockFactory.ROLES)

    assert 0 < historyWriter.read(fs, history, rh2)
    NodeInstance ni2 = rh2.getExistingNodeInstance(addr)
    assert ni2 != null
    NodeEntry ne2 = ni2.get(0)
    assert ne2 !=null
    assert ne2.lastUsed == ne1.lastUsed
  }
    
  @Test
  public void testWriteReadActiveData() throws Throwable {
    RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
    roleHistory.onStart(fs, historyPath)
    String addr = "localhost"
    String addr2 = "rack1server5"
    NodeInstance localhost = roleHistory.getOrCreateNodeInstance(addr)
    NodeEntry orig1 = localhost.getOrCreate(0)
    orig1.lastUsed = 0x10
    NodeInstance rack1server5 = roleHistory.getOrCreateNodeInstance(addr2)
    NodeEntry orig2 = rack1server5.getOrCreate(1)
    orig2.live = 3
    assert !orig2.available
    NodeEntry orig3 = localhost.getOrCreate(1)
    orig3.lastUsed = 0x20
    orig3.live = 1
    assert !orig3.available
    orig3.release()
    assert orig3.available
    roleHistory.dump()
    
    long savetime = 0x0001000
    Path history = roleHistory.saveHistory(savetime)
    assert fs.isFile(history)
    describe("Loaded")
    log.info("testWriteReadActiveData in $history")
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    RoleHistory rh2 = new RoleHistory(MockFactory.ROLES)

    assert 3 == historyWriter.read(fs, history, rh2)
    rh2.dump()
    
    assert rh2.clusterSize == 2;
    NodeInstance ni2 = rh2.getExistingNodeInstance(addr)
    assert ni2 != null
    NodeEntry loadedNE = ni2.get(0)
    assert loadedNE.lastUsed == orig1.lastUsed
    NodeInstance ni2b = rh2.getExistingNodeInstance(addr2)
    assert ni2b != null
    NodeEntry loadedNE2 = ni2b.get(1)
    assert loadedNE2 != null
    assert loadedNE2.lastUsed == savetime

    // now thaw it
    rh2.onThaw();
    describe("thawing")
    rh2.dump();
    List<NodeInstance> available0 = rh2.cloneAvailableList(0)
    assert available0.size() == 1
    
    NodeInstance entry = available0.get(0)
    assert entry.hostname == "localhost"
    assert entry == localhost
    List<NodeInstance> available1 = rh2.cloneAvailableList(1)
    assert available1.size() == 2
    //and verify that even if last used was set, the save time is picked up
    assert entry.get(1).lastUsed == roleHistory.saveTime
  }
  
}
