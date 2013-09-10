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

package org.apache.hadoop.hoya.yarn.client;

import com.beust.jcommander.Parameter;
import groovy.transform.CompileStatic;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hadoop.hoya.providers.HoyaProviderFactory;
import org.apache.hadoop.hoya.tools.PathArgumentConverter;
import org.apache.hadoop.hoya.yarn.CommonArgs;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Args list for JCommanderification
 */

public class ClientArgs extends CommonArgs {

  /**
   * Name of entry class: {@value}
   */
  public static final String CLASSNAME =
    "org.apache.hadoop.hoya.yarn.client.HoyaClient";
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

  @Parameter(names = ARG_AMQUEUE,
             description = "Application Manager Queue Name")
  public String amqueue = "default";

  //--format 
  @Parameter(names = ARG_FORMAT,
             description = "Format for a response: [text|xml|json|properties]")
  public String format = FORMAT_XML;

  //--wait [timeout]
  @Parameter(names = {ARG_WAIT},
             description = "time to wait for an action to complete")
  public int waittime = 0;

  /**
   * --image path
   the full path to a .tar or .tar.gz path containing an HBase image.
   */
  @Parameter(names = ARG_IMAGE,
             description = "The full path to a .tar or .tar.gz path containing the application",
             converter = PathArgumentConverter.class)
  public Path image;

  @Parameter(names = ARG_APP_HOME,
             description = "Home directory of a pre-installed application")
  public String appHomeDir;
  
  @Parameter(names = ARG_PROVIDER,
             description = "Provider of the specific cluster application")
  public String provider = HoyaProviderFactory.DEFAULT_CLUSTER_TYPE;
  
  
  @Parameter(names = {ARG_PERSIST},
             description = "flag to indicate whether a flex change should be persisted (default=true)",
             arity = 1)
  public boolean persist;


  /**
   * This is a listing of the roles to create
   */
  @Parameter(names = {ARG_OPTION, ARG_OPTION_SHORT}, arity = 2,
             description = "option <name> <value>")
  public List<String> optionTuples = new ArrayList<String>(0);

  /**
   * This is a listing of the roles to create
   */
  @Parameter(names = {ARG_ROLE}, arity = 2,
             description = "role <name> <count>")
  public List<String> roleTuples = new ArrayList<String>(0);

  /**
   * All the role option triples
   */
  @Parameter(names = {ARG_ROLEOPT}, arity = 3,
             description = "Role option " + ARG_ROLEOPT + " <role> <name> <option>")
  public List<String> roleOptTriples = new ArrayList<String>(0);


  /**
   * Get the role mapping (may be empty, but never null)
   * @return role mapping
   * @throws BadCommandArgumentsException parse problem
   */
  public Map<String, String> getRoleMap() throws BadCommandArgumentsException {
    return convertTupleListToMap("roles", roleTuples);
  }
  
  public Map<String, String> getOptionsMap() throws BadCommandArgumentsException {
    return convertTupleListToMap("options", optionTuples);
  }
  
  

  /**
   * map of actions -> (explanation, min #of entries [, max no.])
   * If the max no is not given it is assumed to be the same as the min no.
   */

  private static final Map<String, List<Object>> ACTIONS =
    new HashMap<String, List<Object>>();

  static {
    ACTIONS.put(ACTION_BUILD, t("Build a Hoya cluster specification -but do not start it", 1));
    ACTIONS.put(ACTION_CREATE, t("Create a live Hoya cluster", 1));
    ACTIONS.put(ACTION_DESTROY,
                t("Destroy a Hoya cluster (which must be stopped)", 1));
    ACTIONS.put(ACTION_FLEX, t("Flex a Hoya cluster", 1));
    ACTIONS.put(ACTION_GETCONF, t("Get the configuration of a cluster", 1));
//    ACTIONS.put(ACTION_GETSIZE, t("Get the size of a cluster", 1));
    ACTIONS.put(ACTION_EXISTS, t("Probe for a cluster being live", 1));
    ACTIONS.put(ACTION_HELP, t("Print help information", 0));
    ACTIONS.put(ACTION_LIST, t("List running Hoya clusters", 0, 1));
//    ACTIONS.put(ACTION_MIGRATE, t("Migrate a Hoya cluster to a new HBase version", 1));
//    ACTIONS.put(ACTION_PREFLIGHT, t("Perform preflight checks", 0));
    ACTIONS.put(ACTION_RECONFIGURE,
                t("change the configuration of a cluster", 1));
//    ACTIONS.put(ACTION_REIMAGE, t("change the image a cluster uses", 1));
    ACTIONS.put(ACTION_THAW, t("thaw/start a frozen cluster", 1));
    ACTIONS.put(ACTION_STATUS, t("Get the status of a cluster", 1));
    ACTIONS.put(ACTION_FREEZE, t("freeze/suspend a running cluster", 1));
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
  public void applyDefinitions(Configuration conf) {
    super.applyDefinitions(conf);
    //RM
    if (manager != null) {
      LOG.debug("Setting RM to {}", manager);
      conf.set(YarnConfiguration.RM_ADDRESS, manager);
    }
  }

  /**
   * Create a map from a tuple list like
   * ['worker','heapsize','5G','master','heapsize','2M'] into a map
   * ['worker':'2',"master":'1'];
   * Duplicate entries also trigger errors
   * @param description
   * @param list
   * @return
   * @throws BadCommandArgumentsException odd #of arguments received
   */
  public Map<String, Map<String, String>> convertTripleListToMaps(String description,
          List<String> list) throws BadCommandArgumentsException {
    Map<String, Map<String, String>> results =
      new HashMap<String, Map<String, String>>();
    if (list != null && !list.isEmpty()) {
      int size = list.size();
      if (size % 3 != 0) {
        //wrong number of elements, not permitted
        throw new BadCommandArgumentsException(
          ERROR_PARSE_FAILURE + description);
      }
      for (int count = 0; count < size; count += 3) {
        String role = list.get(count);
        String key = list.get(count + 1);
        String val = list.get(count + 2);
        Map<String, String> roleMap = results.get(role);
        if (roleMap == null) {
          //demand create new role map
          roleMap = new HashMap<String, String>();
          results.put(role, roleMap);
        }
        if (roleMap.get(key) != null) {
          throw new BadCommandArgumentsException(
            ERROR_DUPLICATE_ENTRY + description
            + ": for key " + key + " under " + role);
        }
        roleMap.put(key, val);
      }
    }
    return results;
  }


  /**
   * Get the role heap mapping (may be empty, but never null)
   * @return role heap mapping
   * @throws BadCommandArgumentsException parse problem
   */
  public Map<String, Map<String, String>> getRoleOptionMap() throws
                                                     BadCommandArgumentsException {
    return convertTripleListToMaps(ARG_ROLEOPT, roleOptTriples);
  }
}
