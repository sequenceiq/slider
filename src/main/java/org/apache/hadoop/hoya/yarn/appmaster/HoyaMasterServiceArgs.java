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


package org.apache.hadoop.hoya.yarn.appmaster;

import com.beust.jcommander.Parameter;
import org.apache.hadoop.hoya.yarn.CommonArgs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parameters sent by the Client to the AM
 */
public class HoyaMasterServiceArgs extends CommonArgs {
  /**
   * Name of entry class: {@value}
   */
  public static final String CLASSNAME =
    "org.apache.hadoop.hoya.yarn.appmaster.HoyaAppMaster";


  /**
   * Path for the ZK instance (required)
   */
  public static final String ARG_RM_ADDR = "--rm";
  
  public static final String ARG_HOYA_CLUSTER_URI = "--hoya-cluster-uri";
  
  
  /**
   *    Declare the image configuration directory to use when creating
   *    or reconfiguring a hoya cluster.
   *    The path must be on a filesystem visible to all nodes in the
   *    YARN cluster.
   *    Only one configuration directory can be specified.
   */
  @Parameter(names = "--generated_confdir",
             description = "generated configuration directory")
  public String generatedConfdir;

  @Parameter(names = ARG_IMAGE, description = "image", required = false)
  public String image;

  /**
   * This is the URI in the FS to the Hoya cluster; the conf file (and any
   * other cluster-specifics) can be picked up here
   */
  @Parameter(names = ARG_HOYA_CLUSTER_URI, description = "URI to the hoya cluster", required = true)
  public String hoyaClusterURI;


  /**
   * The only action you can do in the MasterService (apart from ask for help)
   * Is create a cluster
   */

  private static final Map<String, List<Object>> ACTIONS =
    new HashMap<String, List<Object>>();

  static {
    ACTIONS.put(ACTION_CREATE, t("create cluster", 1));
    ACTIONS.put(ACTION_HELP, t("Print Help information", 0));
  }


  public HoyaMasterServiceArgs(String[] args) {
    super(args);
  }

  @Override
  public Map<String, List<Object>> getActions() {
    return ACTIONS;
  }


}
