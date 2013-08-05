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

package org.apache.hadoop.hoya.yarn.client

import com.beust.jcommander.Parameter
import groovy.transform.CompileStatic
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hoya.tools.PathArgumentConverter
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.yarn.conf.YarnConfiguration

/**
 * Args list for JCommanderification
 */
@CompileStatic

public class ClientArgs extends CommonArgs {

  /**
   * Name of entry class: {@value}
   */
  public static final String CLASSNAME = "org.apache.hadoop.hoya.yarn.client.HoyaClient";
  /**
   filesystem-uri: {@value}
   */
  public static final String ARG_AMQUEUE = "--amqueue";
  public static final String ARG_AMPRIORITY = "--ampriority";
  //public static final String ARG_FILESYSTEM = "--fs";
  public static final String ARG_FORMAT = "--format";
  public static final String ARG_PERSIST = "--persist";
  public static final String ARG_WAIT = "--wait";



  public static final String FORMAT_XML = "xml";
  public static final String FORMAT_PROPERTIES = "properties";

  @Parameter(names = "--amqueue", description = "Application Manager Queue Name")
  public String amqueue = "default";

  //--format 
  @Parameter(names = "--format", description = "format for a response text|xml|json|properties")
  public String format = FORMAT_XML;

  //--wait [timeout]
  @Parameter(names = "--wait",
      description = "time to wait for an action to complete")
  public int waittime = 0;

  /**
   * --image path
   the full path to a .tar or .tar.gz path containing an HBase image.
   */
  @Parameter(names = "--image",
      description = "the full path to a .tar or .tar.gz path containing an HBase image",
      converter = PathArgumentConverter)
  public Path image;

  @Parameter(names = "--persist",
      description = "flag to indicate whether a flex change should be persisted (default=true)",
      arity = 1)
  public boolean persist;

  /**
   * map of actions -> (explanation, min #of entries [, max no.])
   * If the max no is not given it is assumed to be the same as the min no.
   */

  private static final Map<String, List<Object>> ACTIONS;
  static {
    ACTIONS.put(ACTION_ADDNODE, t("add nodes", 1, 1));
    ACTIONS.put(ACTION_CREATE, t("create cluster", 1));
    ACTIONS.put(ACTION_DESTROY, t("destroy a stopped cluster", 1));
    ACTIONS.put(ACTION_FLEX, t("flex a running cluster", 1));
    ACTIONS.put(ACTION_GETCONF, t("get the configuration of a cluster", 1));
    ACTIONS.put(ACTION_GETSIZE, t("get the size of a cluster", 1));
    ACTIONS.put(ACTION_EXISTS, t("probe for a cluster being live", 1));
    ACTIONS.put(ACTION_HELP, t("Print Help information", 0));
    ACTIONS.put(ACTION_LIST, t("List running cluster", 0, 1));
    ACTIONS.put(ACTION_MIGRATE, t("migrate cluster to a new HBase version", 1));
    ACTIONS.put(ACTION_ADDNODE, t("add nodes", 1));
    ACTIONS.put(ACTION_PREFLIGHT, t("Perform preflight checks", 0));
    ACTIONS.put(ACTION_RECONFIGURE, t("change the configuration of a cluster", 1));
    ACTIONS.put(ACTION_RMNODE, t("remove nodes", 1));
    ACTIONS.put(ACTION_REIMAGE, t("change the image a cluster uses", 1));
    ACTIONS.put(ACTION_START, t("start a cluster", 1));
    ACTIONS.put(ACTION_STATUS, t("Get the status of a cluster", 1));
    ACTIONS.put(ACTION_STOP, t("stop a cluster", 1));
  }

  public ClientArgs(String[] args) {
    super(args);
  }

  public ClientArgs(Collection args) {
    super(args);
  }

  @Override
  public Map<String, List<Object>> getActions() {
    return ACTIONS;
  }

  @Override
  void applyDefinitions(Configuration conf) {
    super.applyDefinitions(conf);
    //RM
    if (manager != null) {
      log.debug("Setting RM to {}", manager);
      conf.set(YarnConfiguration.RM_ADDRESS, manager);
    }
  }
}
