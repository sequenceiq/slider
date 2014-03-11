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
import org.apache.hoya.core.persist.JsonSerDeser;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A conf tree represents one of the configuration trees
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public final class ConfTree {

  protected static final Logger
    log = LoggerFactory.getLogger(ConfTree.class);

  /**
   * version counter
   */
  public String version = CoreKeys.APPLICATION_CONF_VERSION;

  /**
   * Metadata
   */
  public Map<String, Object> metadata;


  /**
   * Global options
   */
  public Map<String, String> global =
    new HashMap<String, String>();


  /**
   * Role options, 
   * role -> option -> value
   */
  public Map<String, Map<String, String>> components =
    new HashMap<String, Map<String, String>>();



  /**
   * Shallow clone
   * @return a shallow clone
   * @throws CloneNotSupportedException
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  @Override
  public String toString() {
    try {
      return toJson();
    } catch (Exception e) {
      log.warn("Failed to convert to JSON ", e);
      return super.toString();
    }
  }

  /**
   * Convert to a JSON string
   * @return a JSON string description
   * @throws IOException Problems mapping/writing the object
   */
  public String toJson() throws IOException,
                                JsonGenerationException,
                                JsonMappingException {
    return JsonSerDeser.toJson(this);
  }

}
