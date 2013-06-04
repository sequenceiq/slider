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

package org.apache.hadoop.hoya.tools

import groovy.transform.CompileStatic
import org.apache.hadoop.conf.Configuration

/**
 * This is a set of static helper methods to work on configurations, 
 * available with the <code>use</code> method.
 * 
 * The only one that adds anything new to the normal use is the 
 * addConfigMap() operation; the others are only there to simplify
 * metaprogramming operations.
 * 
 * To use, create an instance -this triggers method injection
 */
@CompileStatic

class ConfigHelper {

  public static def setConfigEntry(Configuration self, def key, def value) {
    self.set(key.toString(), value.toString())
  }

  public static String getConfigEntry(Configuration self, def key) {
    self.get(key.toString())
  }

  /**
   * Set an entire map full of values
   * @param map map
   * @return nothing
   */
  public static def addConfigMap(Configuration self, Map map) {
    map.each { Map.Entry mapEntry ->
      setConfigEntry(self, mapEntry.key, mapEntry.value)  
    }
  }
}
