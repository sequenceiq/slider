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

package org.apache.hadoop.hoya

import groovy.util.logging.Commons

import java.util.concurrent.atomic.AtomicBoolean

/**
 * This initiates any VM-wide init actions
 */
@Commons
public class HoyaApp {

  AtomicBoolean started = new AtomicBoolean(false)
  /**
   * start any Hoya-specific init-actions
   * @param name name
   */
  public HoyaApp(String name) {
    log.info("Starting Hoya component $name")
    if (started.getAndSet(true)) {
      oneOffInitActions();
    }
  }
  
  private static oneOffInitActions() {
   
  }
  
}
