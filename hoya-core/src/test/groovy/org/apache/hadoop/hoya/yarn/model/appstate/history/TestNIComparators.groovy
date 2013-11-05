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
import org.junit.Test

/**
 * Unit test to verify the comparators sort as expected
 */
class TestNIComparators {

  NodeInstance age1Active4 = instance(1000, 4)
  NodeInstance age2Active2 = instance(1001, 2)
  NodeInstance age3Active0 = instance(1002, 0)
  NodeInstance age4Active1 = instance(1005, 0)
  NodeInstance empty = new NodeInstance("empty", 1)

  List<NodeInstance> nodes = [age2Active2,age4Active1,age1Active4,age3Active0]
  List<NodeInstance> orig = new ArrayList<>(nodes)


  public NodeInstance instance(long age, int live) {
    NodeInstance ni = new NodeInstance("host-${age}-${live}", 1)
    ni.getOrCreate(0).lastUsed = age
    ni.getOrCreate(0).live = live;
    return ni

  }


  public void assertListEquals(List left, List right) {
    assert left.size() == right.size();
    for (int i = 0; i < left.size(); i++) {
      assert  left[0] == right[0]
    }
  }
  
  @Test
  public void testNewerThan() throws Throwable {
  
    Collections.sort(nodes, new NodeInstance.newerThan(0))
    assertListEquals(nodes,
        [age4Active1, age3Active0, age2Active2, age1Active4] )
  }
  
  @Test
  public void testNewerThanNoRole() throws Throwable {
  
    nodes << empty
    Collections.sort(nodes, new NodeInstance.newerThan(0))
    assertListEquals(nodes,
        [age4Active1, age3Active0, age2Active2, age1Active4, empty] )
  }
  
  @Test
  public void testMoreActiveThan() throws Throwable {
  
    Collections.sort(nodes, new NodeInstance.moreActiveThan(0))
    assertListEquals(nodes,
        [age1Active4, age2Active2, age4Active1, age3Active0],)
  }  
  @Test
  public void testMoreActiveThanEmpty() throws Throwable {
    nodes << empty
    Collections.sort(nodes, new NodeInstance.moreActiveThan(0))
    assertListEquals(nodes,
        [age1Active4, age2Active2, age4Active1, age3Active0, empty] )
  }
  
  
}
