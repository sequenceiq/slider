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

package org.apache.hadoop.hoya.yarn.appmaster.state;

import org.apache.hoya.avro.NodeAddress;

/**
 * Create a node instance
 */
public class NodeInstance {

  public final NodeAddress nodeAddress;

  private final NodeEntry[] nodeEntries;

  /**
   * Create an instance and the (empty) array of nodes
   * @param roles role count -the
   */
  public NodeInstance(NodeAddress nodeAddress, int roles) {
    this.nodeAddress = nodeAddress;
    nodeEntries = new NodeEntry[roles];
  }

  /**
   * Get the entry for a role -if present
   * @param role role index
   * @return the entry
   * @throws ArrayIndexOutOfBoundsException if the role is out of range
   */
  public NodeEntry get(int role) {
   return nodeEntries[role] ;
  }
  /**
   * Get the entry for a role -if present
   * @param role role index
   * @return the entry
   * @throws ArrayIndexOutOfBoundsException if the role is out of range
   */
  public NodeEntry getOrCreate(int role) {
    NodeEntry entry = nodeEntries[role];
    if (entry == null) {
      entry = new NodeEntry();
      nodeEntries[role] = entry;
    }
    return entry;
  }

  /**
   * Get a clone of the node entries; changes to it are
   * not reflected, though they are if you edit the entries themselves
   * @return a copy of the array
   */
  public NodeEntry[] cloneNodeEntries() {
    return nodeEntries.clone();
  }

  /**
   * Get the entry for a role -and remove it if present
   * @param role the role index
   * @return the entry that WAS there
   */
  public NodeEntry remove(int role) {
    NodeEntry nodeEntry = get(role);
    nodeEntries[role] = null;
    return nodeEntry;
  }

  public void set(int role, NodeEntry nodeEntry) {
    nodeEntries[role] = nodeEntry;
  }
}
