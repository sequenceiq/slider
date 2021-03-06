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

package org.apache.hoya.core.persist;

import org.apache.hadoop.fs.Path;
import org.apache.hoya.HoyaKeys;

/**
 * Build up all the paths of an instance relative to the supplied instance
 * directory.
 */
public class InstancePaths {

  public final Path instanceDir;
  public final Path snapshotConfPath;
  public final Path generatedConfPath;
  public final Path historyPath;
  public final Path dataPath;
  public final Path tmpPath;
  public final Path tmpPathAM;

  public InstancePaths(Path instanceDir) {
    this.instanceDir = instanceDir;
    snapshotConfPath =
      new Path(instanceDir, HoyaKeys.SNAPSHOT_CONF_DIR_NAME);
    generatedConfPath =
      new Path(instanceDir, HoyaKeys.GENERATED_CONF_DIR_NAME);
    historyPath = new Path(instanceDir, HoyaKeys.HISTORY_DIR_NAME);
    dataPath = new Path(instanceDir, HoyaKeys.DATA_DIR_NAME);
    tmpPath = new Path(instanceDir, HoyaKeys.TMP_DIR_PREFIX);
    tmpPathAM = new Path(tmpPath, "appmaster");
  }

  @Override
  public String toString() {
    return "instance at " + instanceDir;
  }
}
