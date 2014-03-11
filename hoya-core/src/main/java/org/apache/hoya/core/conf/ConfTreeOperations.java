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

import org.apache.hoya.core.CoreKeys;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.tools.HoyaUtils;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ConfTreeOperations {

  public final ConfTree tree;
  private MapOperations globalOptions;


  public ConfTreeOperations(ConfTree tree) {
    assert tree != null : "null tree";
    assert tree.components != null : "null tree components";
    this.tree = tree;
    globalOptions = new MapOperations(tree.global);
  }


  /**
   * Validate the configuration
   * @throws BadConfigException
   */
  public void validate() throws BadConfigException {
    String version = tree.schema;
    if (version == null) {
      throw new BadConfigException("'version' undefined");
    }
    if (!CoreKeys.SCHEMA.equals(version)) {
      throw new BadConfigException(
        "version %s incompatible with supported version %s",
        version,
        CoreKeys.SCHEMA);
    }
  }

  /**
   * Resolve a ConfTree by mapping all global options into each component
   * -if there is none there already
   */
  public void resolve() {
    for (Map.Entry<String, Map<String, String>> comp : tree.components.entrySet()) {
      mergeOptions(comp.getValue());
    }
  }

  /**
   * Merge any options
   * @param component dest values
   */
  public void mergeOptions(Map<String, String> component) {
    HoyaUtils.mergeMapsIgnoreDuplicateKeys(component, tree.global);
  }

  /**
   * Get operations on the global set
   * @return a wrapped map
   */
  public MapOperations getGlobalOptions() {

    return globalOptions;
  }


  /**
   * look up a component and return its options
   * @param component component name
   * @return component mapping or null
   */
  public MapOperations getComponentOperations(String component) {
    Map<String, String> instance = tree.components.get(component);
    if (instance != null) {
      return new MapOperations(instance);
    }
    return null;
  }

  /**
   * Get a component -adding it to the components map if
   * none with that name exists
   * @param name role
   * @return role mapping
   */
  public MapOperations getOrAddComponent(String name) {
    MapOperations operations = getComponentOperations(name);
    if (operations != null) {
      return operations;
    }
    //create a new instances
    Map<String, String> map = new HashMap<String, String>();
    tree.components.put(name, map);
    return new MapOperations(map);
  }


  /*
   * return the Set of names names
   */
  @JsonIgnore
  public Set<String> getComponentNames() {
    return new HashSet<String>(tree.components.keySet());
  }

  /**
   * Get a component whose presence is mandatory
   * @param name component name
   * @return the mapping
   * @throws BadConfigException if the name is not there
   */
  public MapOperations getMandatoryComponent(String name) throws
                                                          BadConfigException {
    MapOperations ops = getComponentOperations(name);
    if (ops == null) {
      throw new BadConfigException("Missing component " + name);
    }
    return ops;
  }
}
