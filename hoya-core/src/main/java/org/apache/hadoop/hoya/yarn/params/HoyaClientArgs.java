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
import org.apache.hadoop.hoya.HoyaExitCodes;
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hadoop.hoya.exceptions.HoyaException;
import org.apache.hadoop.hoya.yarn.HoyaActions;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hoya Client CLI Args
 */

public class HoyaClientArgs extends CommonArgs {

  /*
   
   All the arguments for specific actions
  
   */
  /**
   * This is not bonded to jcommander, it is set up
   * after the construction to point to the relevant
   * entry
   */
  AbstractClusterBuildingActionArgs buildingActionArgs;
  ActionBuildArgs actionBuildArgs = new ActionBuildArgs();
  ActionCreateArgs actionCreateArgs = new ActionCreateArgs();
  ActionDestroyArgs actionDestroyArgs = new ActionDestroyArgs();
  ActionExistsArgs actionExistsArgs = new ActionExistsArgs();
  ActionFlexArgs actionFlexArgs = new ActionFlexArgs();
  ActionForceKillArgs actionForceKillArgs = new ActionForceKillArgs();
  ActionFreezeArgs actionFreezeArgs = new ActionFreezeArgs();
  ActionGetConfArgs actionGetConfArgs = new ActionGetConfArgs();
  ActionListArgs actionListArgs = new ActionListArgs();
  ActionMonitorArgs actionMonitorArgs = new ActionMonitorArgs();
  ActionStatusArgs actionStatusArgs = new ActionStatusArgs();
  ActionThawArgs  actionThawArgs = new ActionThawArgs();
  
  
 
  //--format 
  @Parameter(names = ARG_FORMAT,
             description = "Format for a response: [xml|properties]")
  private String format = FORMAT_XML;

  //--wait [timeout]
  @Parameter(names = {ARG_WAIT},
             description = "time to wait for an action to complete")
  private int waittime = 0;
  
  
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
    ACTIONS.put(ACTION_BUILD, tuple(
      DESCRIBE_ACTION_BUILD, 1));
    ACTIONS.put(ACTION_CREATE, tuple(DESCRIBE_ACTION_CREATE, 1));
    ACTIONS.put(ACTION_DESTROY,
                tuple(DESCRIBE_ACTION_DESTROY,
                      1));
    ACTIONS.put(ACTION_EMERGENCY_FORCE_KILL, tuple(
      DESCRIBE_ACTION_FORCE_KILL, 1));
    ACTIONS.put(ACTION_EXISTS, tuple(DESCRIBE_ACTION_EXISTS,
                                     1));
    ACTIONS.put(ACTION_FLEX, tuple(DESCRIBE_ACTION_FLEX, 1));
    ACTIONS.put(ACTION_FREEZE, tuple(DESCRIBE_ACTION_FREEZE,
                                     1));
    ACTIONS.put(ACTION_GETCONF, tuple(
      DESCRIBE_ACTION_GETCONF, 1));
//    ACTIONS.put(ACTION_GETSIZE, t("Get the size of a cluster", 1));
    ACTIONS.put(ACTION_HELP, tuple(DESCRIBE_ACTION_HELP, 0));
    ACTIONS.put(ACTION_LIST, triple(DESCRIBE_ACTION_LIST, 0, 1));
    ACTIONS.put(ACTION_MONITOR, tuple(DESCRIBE_ACTION_MONITOR, 1));
//    ACTIONS.put(ACTION_MIGRATE, t("Migrate a Hoya cluster to a new HBase version", 1));
//    ACTIONS.put(ACTION_PREFLIGHT, t("Perform preflight checks", 0));
//    ACTIONS.put(ACTION_RECONFIGURE,
//                triple("change the configuration of a cluster", 1));
//    ACTIONS.put(ACTION_REIMAGE, t("change the image a cluster uses", 1));
    ACTIONS.put(ACTION_STATUS, tuple(DESCRIBE_ACTION_STATUS, 1));
    ACTIONS.put(ACTION_THAW, tuple(DESCRIBE_ACTION_THAW, 1));
    ACTIONS.put(ACTION_USAGE, tuple(DESCRIBE_ACTION_HELP, 0));
  }

  public HoyaClientArgs(String[] args) {
    super(args);
  }

  public HoyaClientArgs(Collection args) {
    super(args);
  }

  @Override
  protected void addActionArguments() {

    addActions(
      actionBuildArgs,
      actionCreateArgs,
      actionDestroyArgs,
      actionExistsArgs,
      actionFlexArgs,
      actionForceKillArgs,
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

  public AbstractClusterBuildingActionArgs getBuildingActionArgs() {
    return buildingActionArgs;
  }

  public ActionBuildArgs getActionBuildArgs() {
    return actionBuildArgs;
  }

  public ActionCreateArgs getActionCreateArgs() {
    return actionCreateArgs;
  }

  public ActionDestroyArgs getActionDestroyArgs() {
    return actionDestroyArgs;
  }

  public ActionExistsArgs getActionExistsArgs() {
    return actionExistsArgs;
  }

  public ActionFlexArgs getActionFlexArgs() {
    return actionFlexArgs;
  }

  public ActionForceKillArgs getActionForceKillArgs() {
    return actionForceKillArgs;
  }

  public ActionFreezeArgs getActionFreezeArgs() {
    return actionFreezeArgs;
  }

  public ActionGetConfArgs getActionGetConfArgs() {
    return actionGetConfArgs;
  }

  public ActionListArgs getActionListArgs() {
    return actionListArgs;
  }

  public ActionMonitorArgs getActionMonitorArgs() {
    return actionMonitorArgs;
  }

  public ActionStatusArgs getActionStatusArgs() {
    return actionStatusArgs;
  }

  public ActionThawArgs getActionThawArgs() {
    return actionThawArgs;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  /**
   * Get the wait time -provided the bonded arg implements 
   * {@link WaitTimeAccessor}
   * @return the wait time in seconds
   * @throws ClassCastException if the action is of a wrong type
   */
  public int getWaittime() {
    return ((WaitTimeAccessor)getCoreAction()).getWaittime();
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


  /**
   * All the role option triples
   */
  public List<String> getRoleOptTriples() {
    return roleOptTriples;
  }


  @Override
  public void applyAction() throws HoyaException {
    String action = getAction();
    if (HoyaActions.ACTION_BUILD.equals(action)) {
      bindCoreAction(actionBuildArgs);
      //its a builder, so set those actions too
      buildingActionArgs = actionBuildArgs;
      
    } else if (HoyaActions.ACTION_CREATE.equals(action)) {
      bindCoreAction(actionCreateArgs);
      //its a builder, so set those actions too
      buildingActionArgs = actionCreateArgs;
      
    } else if (HoyaActions.ACTION_FREEZE.equals(action)) {
      bindCoreAction(actionFreezeArgs);

    } else if (HoyaActions.ACTION_THAW.equals(action)) {
      bindCoreAction(actionThawArgs);

    } else if (HoyaActions.ACTION_DESTROY.equals(action)) {
      bindCoreAction(actionDestroyArgs);

    } else if (HoyaActions.ACTION_EMERGENCY_FORCE_KILL.equals(action)) {
      bindCoreAction(actionForceKillArgs);

    } else if (HoyaActions.ACTION_EXISTS.equals(action)) {
      bindCoreAction(actionExistsArgs);

    } else if (HoyaActions.ACTION_FLEX.equals(action)) {
      bindCoreAction(actionFlexArgs);

    } else if (HoyaActions.ACTION_GETCONF.equals(action)) {
      bindCoreAction(actionGetConfArgs);
    } else if (HoyaActions.ACTION_HELP.equals(action) ||
               HoyaActions.ACTION_USAGE.equals(action)) {
      throw new HoyaException(HoyaExitCodes.EXIT_UNIMPLEMENTED,
                              "Unimplemented: " + action);
    } else if (HoyaActions.ACTION_LIST.equals(action)) {
      bindCoreAction(actionListArgs);

    } else if (HoyaActions.ACTION_MONITOR.equals(action)) {
      bindCoreAction(actionMonitorArgs);

    } else if (HoyaActions.ACTION_STATUS.equals(action)) {
      bindCoreAction(actionStatusArgs);

    } else {
      throw new HoyaException(HoyaExitCodes.EXIT_UNIMPLEMENTED,
                              "Unimplemented: " + action);
    }
  }
}
