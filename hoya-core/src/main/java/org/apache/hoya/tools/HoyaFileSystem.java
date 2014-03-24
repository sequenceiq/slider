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

package org.apache.hoya.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.core.persist.Filenames;
import org.apache.hoya.exceptions.BadClusterStateException;
import org.apache.hoya.exceptions.ErrorStrings;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.exceptions.UnknownClusterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Extends Core Filesystem with operations to manipulate ClusterDescription
 * persistent state
 */
public class HoyaFileSystem extends CoreFileSystem {

  public HoyaFileSystem(FileSystem fileSystem,
                        Configuration configuration) {
    super(fileSystem, configuration);
  }

  public HoyaFileSystem(Configuration configuration) throws IOException {
    super(configuration);
  }


}
