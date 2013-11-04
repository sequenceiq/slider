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
import org.apache.hadoop.hoya.HoyaKeys
import org.apache.hadoop.hoya.avro.RoleHistoryWriter
import org.apache.hadoop.hoya.yarn.appmaster.state.NodeEntry
import org.apache.hadoop.hoya.yarn.appmaster.state.NodeInstance
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleHistory
import org.apache.hadoop.hoya.yarn.model.mock.BaseMockAppStateTest
import org.apache.hadoop.hoya.yarn.model.mock.MockFactory
import org.junit.Test

import java.util.regex.Matcher
import java.util.regex.Pattern

@Slf4j
@CompileStatic
class TestHistoryRWOrdering extends BaseMockAppStateTest {

  @Override
  String getTestName() {
    return "TestHistoryRWOrdering"
  }

    
  /**
   * This tests regexp pattern matching. It uses the current time so isn't
   * repeatable -but it does test a wider range of values in the process
   * @throws Throwable
   */
  @Test
  public void testPatternRoundTrip() throws Throwable {
    describe "test pattern matching of names"
    long value=System.currentTimeMillis()
    String name = String.format(HoyaKeys.HISTORY_FILENAME_CREATION_PATTERN,value)
    String matchpattern = HoyaKeys.HISTORY_FILENAME_MATCH_PATTERN
    Pattern pattern = Pattern.compile(matchpattern)
    Matcher matcher = pattern.matcher(name);
    if (!matcher.find()) {
      throw new Exception("No match for pattern $matchpattern in $name")
    }
  }


  @Test
  public void testWriteSequenceReadData() throws Throwable {
    describe "test that if multiple entries are written, the newest is picked up"
    long time = System.currentTimeMillis();

    RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
    roleHistory.onStart(fs, historyPath)
    String addr = "localhost"
    NodeInstance instance = roleHistory.getOrCreateNodeInstance(addr)
    NodeEntry ne1 = instance.getOrCreate(0)
    ne1.lastUsed = 0xf00d

    Path history1 = roleHistory.saveHistory(time++)
    Path history2 = roleHistory.saveHistory(time++)
    Path history3 = roleHistory.saveHistory(time++)
    
    
    
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    
    List<Path> entries = historyWriter.findAllHistoryEntries(  fs, historyPath)
    assert entries.size() == 3
    assert entries[0] == history3
    assert entries[1] == history2
    assert entries[2] == history1
  }
  
}
