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

package org.apache.hoya.core.build;

import org.apache.hadoop.fs.Path;
import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.core.persist.ConfPersister;
import org.apache.hoya.core.persist.LockAcquireFailedException;
import org.apache.hoya.exceptions.BadClusterStateException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.tools.CoreFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class InstanceLoader {
  protected static final Logger log =
    LoggerFactory.getLogger(InstanceLoader.class);

  public static AggregateConf loadInstanceDefinition(
    CoreFileSystem hoyaFileSystem,
    Path clusterDirectory) throws
                                                                      IOException,
                                                                      HoyaException {
    AggregateConf instanceDefinition = new AggregateConf();
    ConfPersister persister =
      new ConfPersister(hoyaFileSystem, clusterDirectory);
    try {
      persister.load(instanceDefinition);
    } catch (LockAcquireFailedException e) {
      log.debug("Lock acquisition failure of {}", clusterDirectory, e);

      throw new BadClusterStateException(
        "Application at %s is locked for reading",
        clusterDirectory.toString());
    }
    return instanceDefinition;
  }

}
