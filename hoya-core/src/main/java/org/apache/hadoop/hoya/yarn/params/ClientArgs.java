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
import org.apache.hadoop.hoya.exceptions.ErrorStrings;
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
  private final ActionHelpArgs actionHelpArgs = new ActionHelpArgs();


  public ClientArgs(String[] args) {
    super(args);
  }

  public ClientArgs(Collection args) {
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
      actionStatusArgs,
      actionThawArgs,
      actionHelpArgs);
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

  /**
   * Get the wait time -provided the bonded arg implements 
   * {@link WaitTimeAccessor}
   * @return the wait time in seconds
   * @throws ClassCastException if the action is of a wrong type
   */
  public int getWaittime() {
    return ((WaitTimeAccessor)getCoreAction()).getWaittime();
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
      bindCoreAction(actionHelpArgs);
    } else if (HoyaActions.ACTION_LIST.equals(action)) {
      bindCoreAction(actionListArgs);

    } else if (HoyaActions.ACTION_MONITOR.equals(action)) {
      bindCoreAction(actionMonitorArgs);

    } else if (HoyaActions.ACTION_STATUS.equals(action)) {
      bindCoreAction(actionStatusArgs);
    
    } else if (HoyaActions.ACTION_STATUS.equals(action)) {
      bindCoreAction(actionStatusArgs);
    } else if (action == null || action.isEmpty()) {
      throw new BadCommandArgumentsException(ErrorStrings.ERROR_NO_ACTION);

    } else {
      throw new BadCommandArgumentsException(ErrorStrings.ERROR_UNKNOWN_ACTION
                                             + " " + action );
    }
  }
}
