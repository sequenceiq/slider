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

package org.apache.hadoop.hoya.api;

import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

/**
 * API for AM operations
 */
public interface HoyaAppMasterProtocol extends VersionedProtocol {
  public static final long versionID = 0x01;

  public void stopCluster() throws IOException;

  public boolean flexNodes(int workers) throws IOException;

  public String getClusterStatus() throws IOException;

  public static final String STAT_CONTAINERS_REQUESTED = "containers.requested";
  public static final String STAT_CONTAINERS_ALLOCATED = "containers.allocated";
  public static final String STAT_CONTAINERS_COMPLETED = "containers.completed";
  public static final String STAT_CONTAINERS_FAILED = "containers.failed";
  public static final String STAT_CONTAINERS_STARTED =
    "containers.start.started";
  public static final String STAT_CONTAINERS_STARTED_FAILED =
    "containers.start.failed";

}
