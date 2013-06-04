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
/**
 * A class that provides array access to system properties.
 *
 * example
 * <code>
 *   def home = Sysprops['java.home']
 *   Sysprops['timeout'] = 24
 * <code>
 */

final class Sysprops {

  static String getAt(String k) {
    return System.getProperty(k)
  }

  static setAt(String k, def v) {
    System.setProperty(k, v.toString())
  }

  /**
   * Get a mandatory property. Raises an assertion if it is 
   * missing.
   * @param k key
   * @return the value
   */
  static String mandatory(String k) {
    def v = getAt(k)
    if (v == null) {
      throw new MissingArgException("Missing Environment variable $k")
    }
    v
  }


}
