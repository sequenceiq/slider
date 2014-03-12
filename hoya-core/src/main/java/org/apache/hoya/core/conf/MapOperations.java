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

package org.apache.hoya.core.conf;

import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.tools.HoyaUtils;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Standard map operations.
 *
 * This delegates the standard map interface to the map passed in,
 * so it can be used to add more actions to the map.
 */
public class MapOperations implements Map<String, String> {


  /**
   * Global options
   */
  public final Map<String, String> options;


  /**
   * Create an instance
   * @param options
   */
  public MapOperations(Map<String, String> options) {
    assert options != null : "null map";
    this.options = options;
  }


  /**
   * Get a cluster option or value
   *
   * @param key
   * @param defVal
   * @return option in map or the default
   */
  public String getOption(String key, String defVal) {
    String val = options.get(key);
    return val != null ? val : defVal;
  }


  /**
   * Get a cluster option or value
   *
   * @param key
   * @return the value
   * @throws BadConfigException if the option is missing
   */
  public String getMandatoryOption(String key) throws BadConfigException {
    String val = options.get(key);
    if (val == null) {
      throw new BadConfigException("Missing option " + key);
    }
    return val;
  }

  /**
   * Get an integer option; use {@link Integer#decode(String)} so as to take hex
   * oct and bin values too.
   *
   * @param option option name
   * @param defVal default value
   * @return parsed value
   * @throws NumberFormatException if the role could not be parsed.
   */
  public int getOptionInt(String option, int defVal) {
    String val = getOption(option, Integer.toString(defVal));
    return Integer.decode(val);
  }

  /**
   * Verify that an option is set: that is defined AND non-empty
   * @param key
   * @throws BadConfigException
   */
  public void verifyOptionSet(String key) throws BadConfigException {
    if (HoyaUtils.isUnset(getOption(key, null))) {
      throw new BadConfigException("Unset option %s", key);
    }
  }

  public int size() {
    return options.size();
  }

  public boolean isEmpty() {
    return options.isEmpty();
  }

  public boolean containsValue(Object value) {
    return options.containsValue(value);
  }

  public boolean containsKey(Object key) {
    return options.containsKey(key);
  }

  public String get(Object key) {
    return options.get(key);
  }

  public String put(String key, String value) {
    return options.put(key, value);
  }

  public String remove(Object key) {
    return options.remove(key);
  }

  public void putAll(Map<? extends String, ? extends String> m) {
    options.putAll(m);
  }

  public void clear() {
    options.clear();
  }

  public Set<String> keySet() {
    return options.keySet();
  }

  public Collection<String> values() {
    return options.values();
  }

  public Set<Map.Entry<String, String>> entrySet() {
    return options.entrySet();
  }

  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  public boolean equals(Object o) {
    return options.equals(o);
  }

  @Override
  public int hashCode() {
    return options.hashCode();
  }
}
