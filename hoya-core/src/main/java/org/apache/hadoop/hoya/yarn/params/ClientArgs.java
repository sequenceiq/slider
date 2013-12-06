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

package org.apache.hadoop.hoya.yarn.params;

import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hadoop.hoya.providers.HoyaProviderFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hoya Client CLI Args
 */

public class ClientArgs extends CommonArgs {

  /*
   
   All the arguments for specific actions
  
   */
  /**
   * This is not bonded to jcommander, it is set up
   * after the construction to point to the relevant
   * entry
   */
  AbstractClusterBuildingActionArgs buildingActionArgs;
  ActionBuildArgs actionBuildArgs;
  ActionCreateArgs actionCreateArgs;
  ActionDestroyArgs actionDestroyArgs;
  ActionExistsArgs actionExistsArgs;
  ActionFlexArgs actionFlexArgs;
  ActionForceKillArgs actionForceKillArgs;
  ActionFreezeArgs actionFreezeArgs;
  ActionGetConfArgs actionGetConfArgs;
  ActionListArgs actionListArgs;
  ActionMonitorArgs actionMonitorArgs;
  ActionStatusArgs actionStatusArgs;
  
  //--format 
  @Parameter(names = ARG_FORMAT,
             description = "Format for a response: [xml|properties]")
  private String format = FORMAT_XML;

  //--wait [timeout]
  @Parameter(names = {ARG_WAIT},
             description = "time to wait for an action to complete")
  private int waittime = 0;

  @Parameter(names = ARG_IMAGE,
             description = "The full path to a .tar or .tar.gz path containing the application",
             converter = PathArgumentConverter.class)
  private Path image;

  @Parameter(names = ARG_APP_HOME,
             description = "Home directory of a pre-installed application")
  private String appHomeDir;
  
  @Parameter(names = ARG_PROVIDER,
             description = "Provider of the specific cluster application")
  private String provider = HoyaProviderFactory.DEFAULT_CLUSTER_TYPE;
  
  
  @Parameter(names = {ARG_PERSIST},
             description = "flag to indicate whether a flex change should be persisted (default=true)",
             arity = 1)
  private boolean persist;

  @Parameter(names = {ARG_OPTION, ARG_OPTION_SHORT}, arity = 2,
             description = "option <name> <value>")
  private List<String> optionTuples = new ArrayList<String>(0);

  @Parameter(names = {ARG_ROLE}, arity = 2,
             description = "role <name> <count>")
  private List<String> roleTuples = new ArrayList<String>(0);

  @Parameter(names = {ARG_ROLEOPT}, arity = 3,
             description = "Role option " + ARG_ROLEOPT + " <role> <name> <option>")
  private List<String> roleOptTriples = new ArrayList<String>(0);


  /**
   * Get the role mapping (may be empty, but never null)
   * @return role mapping
   * @throws BadCommandArgumentsException parse problem
   */
  public Map<String, String> getRoleMap() throws BadCommandArgumentsException {
    return convertTupleListToMap("roles", getRoleTuples());
  }
  
  public Map<String, String> getOptionsMap() throws BadCommandArgumentsException {
    return convertTupleListToMap("options", getOptionTuples());
  }
  
  
  /**
   * map of actions -> (explanation, min #of entries [, max no.])
   * If the max no is not given it is assumed to be the same as the min no.
   */

  private static final Map<String, List<Object>> ACTIONS =
    new HashMap<String, List<Object>>();

  static {
    ACTIONS.put(ACTION_BUILD, triple(
      DESCRIBE_ACTION_BUILD, 1));
    ACTIONS.put(ACTION_CREATE, triple(DESCRIBE_ACTION_CREATE, 1));
    ACTIONS.put(ACTION_DESTROY,
                triple(DESCRIBE_ACTION_DESTROY,
                              1));
    ACTIONS.put(ACTION_EMERGENCY_FORCE_KILL, triple(
      DESCRIBE_ACTION_FORCE_KILL, 1));
    ACTIONS.put(ACTION_EXISTS, triple(DESCRIBE_ACTION_EXISTS,
                                             1));
    ACTIONS.put(ACTION_FLEX, triple(DESCRIBE_ACTION_FLEX, 1));
    ACTIONS.put(ACTION_FREEZE, triple(DESCRIBE_ACTION_FREEZE,
                                             1));
    ACTIONS.put(ACTION_GETCONF, triple(
      DESCRIBE_ACTION_GETCONF, 1));
//    ACTIONS.put(ACTION_GETSIZE, t("Get the size of a cluster", 1));
    ACTIONS.put(ACTION_HELP, triple(DESCRIBE_ACTION_HELP, 0));
    ACTIONS.put(ACTION_LIST, triple(DESCRIBE_ACTION_LIST, 0, 1));
    ACTIONS.put(ACTION_MONITOR, triple(DESCRIBE_ACTION_MONITOR, 1));
//    ACTIONS.put(ACTION_MIGRATE, t("Migrate a Hoya cluster to a new HBase version", 1));
//    ACTIONS.put(ACTION_PREFLIGHT, t("Perform preflight checks", 0));
//    ACTIONS.put(ACTION_RECONFIGURE,
//                triple("change the configuration of a cluster", 1));
//    ACTIONS.put(ACTION_REIMAGE, t("change the image a cluster uses", 1));
    ACTIONS.put(ACTION_STATUS, triple(DESCRIBE_ACTION_STATUS, 1));
    ACTIONS.put(ACTION_THAW, triple(DESCRIBE_ACTION_THAW, 1));
    ACTIONS.put(ACTION_USAGE, triple(DESCRIBE_ACTION_HELP, 0));
  }

  public ClientArgs(String[] args) {
    super(args);
  }

  public ClientArgs(Collection args) {
    super(args);
  }

  @Override
  protected void addActionArguments() {
    super.addActionArguments();
    actionBuildArgs = new ActionBuildArgs();
    actionCreateArgs = new ActionCreateArgs();
    actionDestroyArgs = new ActionDestroyArgs();
    actionExistsArgs = new ActionExistsArgs();
    actionFlexArgs = new ActionFlexArgs();
    actionForceKillArgs = new ActionForceKillArgs();
    actionFreezeArgs = new ActionFreezeArgs();
    actionGetConfArgs = new ActionGetConfArgs();
    actionListArgs = new ActionListArgs();
    actionMonitorArgs = new ActionMonitorArgs();
    actionStatusArgs = new ActionStatusArgs();
    addActions(
      actionBuildArgs,
      actionCreateArgs,
      actionDestroyArgs,
      actionExistsArgs,
      actionFlexArgs,
      actionFreezeArgs,
      actionGetConfArgs,
      actionListArgs,
      actionMonitorArgs,
      actionStatusArgs);


  }

  @Override
  public Map<String, List<Object>> getActions() {
    return ACTIONS;
  }

  @Override
  public void applyDefinitions(Configuration conf) throws
                                                   BadCommandArgumentsException {
    super.applyDefinitions(conf);
    //RM
    if (getManager() != null) {
      log.debug("Setting RM to {}", getManager());
      conf.set(YarnConfiguration.RM_ADDRESS, getManager());
    }
  }


  /**
   * Get the role heap mapping (may be empty, but never null)
   * @return role heap mapping
   * @throws BadCommandArgumentsException parse problem
   */
  public Map<String, Map<String, String>> getRoleOptionMap() throws
                                                     BadCommandArgumentsException {
    return convertTripleListToMaps(ARG_ROLEOPT, getRoleOptTriples());
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public int getWaittime() {
    return waittime;
  }

  public void setWaittime(int waittime) {
    this.waittime = waittime;
  }

  /**
   * --image path
   the full path to a .tar or .tar.gz path containing an HBase image.
   */
  public Path getImage() {
    return image;
  }

  public void setImage(Path image) {
    this.image = image;
  }

  public String getAppHomeDir() {
    return appHomeDir;
  }

  public void setAppHomeDir(String appHomeDir) {
    this.appHomeDir = appHomeDir;
  }

  public String getProvider() {
    return provider;
  }

  public void setProvider(String provider) {
    this.provider = provider;
  }

  public boolean isPersist() {
    return persist;
  }

  public void setPersist(boolean persist) {
    this.persist = persist;
  }

  /**
   * This is a listing of the roles to create
   */
  public List<String> getOptionTuples() {
    return optionTuples;
  }

  public void setOptionTuples(List<String> optionTuples) {
    this.optionTuples = optionTuples;
  }

  /**
   * This is a listing of the roles to create
   */
  public List<String> getRoleTuples() {
    return roleTuples;
  }

  public void setRoleTuples(List<String> roleTuples) {
    this.roleTuples = roleTuples;
  }

  /**
   * All the role option triples
   */
  public List<String> getRoleOptTriples() {
    return roleOptTriples;
  }

  public void setRoleOptTriples(List<String> roleOptTriples) {
    this.roleOptTriples = roleOptTriples;
  }
}
