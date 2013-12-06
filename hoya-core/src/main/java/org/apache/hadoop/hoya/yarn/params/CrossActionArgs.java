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

package org.apache.hadoop.hoya.yarn.params;

import com.beust.jcommander.Parameter;
import org.apache.hadoop.hoya.yarn.Arguments;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * These are arguments that can be applied to any of the
 * Hoya actions; it will be inherited by them (or delegated, depending
 * on how we choose to work with it
 */
public class CrossActionArgs extends ArgOps implements Arguments {

  /**
   * URI of the filesystem
   */
  @Parameter(names = {ARG_FILESYSTEM, ARG_FILESYSTEM_LONG},
             description = "Filesystem URI",
             converter = URIArgumentConverter.class)
  public URI filesystemURL;


  @Parameter(names = {"--m", ARG_MANAGER},
             description = "hostname:port of the YARN resource manager")
  public String manager;
  
  /**
   -D name=value

   Define an HBase configuration option which overrides any options in
   the configuration XML files of the image or in the image configuration
   directory. The values will be persisted.
   Configuration options are only passed to the cluster when creating or reconfiguring a cluster.

   */

  @Parameter(names = ARG_DEFINE, arity = 1, description = "Definitions")
  public List<String> definitions = new ArrayList<String>();
  public Map<String, String> definitionMap = new HashMap<String, String>();
  /**
   * System properties
   */
  @Parameter(names = {ARG_SYSPROP}, arity = 1,
             description = "system properties in the form name value" +
                           " These are set after the JVM is started.")
  public List<String> sysprops = new ArrayList<String>(0);
  public Map<String, String> syspropsMap = new HashMap<String, String>();


}
