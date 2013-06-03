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

package org.apache.hadoop.hoya.yarn

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import com.beust.jcommander.ParameterException
import groovy.transform.CompileStatic
import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hoya.HoyaExceptions

/**
 * This class contains the common argument set for all tne entry points,
 * and the core parsing logic to verify that the action is on the list
 * of allowed actions -and that the remaining number of arguments is
 * in the range allowed
 */
@Commons
@CompileStatic

class CommonArgs {
  public static final String ARG_ACTION = '--action'
  public static final String ARG_CONFDIR = '--confdir'
  public static final String ARG_DEBUG = '--debug'
  public static final String ARG_IMAGE = '--image'
  public static final String ARG_MAX = '--max'
  public static final String ARG_MIN = '--min'
  public static final String ARG_NAME = '--name'
  public static final String ARG_PATH = '--path'
  public static final String ARG_TEST = '--Xtest'
  public static final String ARG_USER = '--user'

  public static final String ARG_ZOOKEEPER = '--zookeeper'
  public static final String ERROR_NO_ACTION = "No action specified"
  public static final String ERROR_UNKNOWN_ACTION = "Unknown command: "
  public static final String ERROR_NOT_ENOUGH_ARGUMENTS = "Not enough arguments for action: "
  /**
   * All the remaining values after argument processing
   */
  public static final String ERROR_TOO_MANY_ARGUMENTS = "Too many arguments for action: "

  /**
   * Actions.
   * Only some of these are supported by specific Hoya Services; they
   * are listed in the common args to ensure the names are consistent
   */
  static final String ACTION_ADDNODE = "addnode"
  static final String ACTION_CREATE = "create"
  static final String ACTION_GETSIZE = "getsize"
  static final String ACTION_HELP = "help"
  static final String ACTION_ISLIVE = "islive"
  static final String ACTION_LIST = "list"
  static final String ACTION_MIGRATE = "migrate"
  static final String ACTION_PREFLIGHT = "preflight"
  static final String ACTION_RECONFIGURE = "reconfigure"
  static final String ACTION_REIMAGE = "reimage"
  static final String ACTION_RMNODE = "rmnode"
  static final String ACTION_START = "start"
  static final String ACTION_STATUS = "status"
  static final String ACTION_STOP = "stop"

  @Parameter
  List<String> parameters = new ArrayList<String>();

  @Parameter(names = '--debug', description = "Debug mode")
  boolean debug = false;

  @Parameter(names = '--help', help = true)
  private boolean help;

  @Parameter(names = '--Xtest', description = "Test mode")
  boolean testmode = false;

  @Parameter(names = "--user",
      description = "username if not self")
  String user = "unknown";
  
  @Parameter(names = "--zookeeper",
      description = "Zookeeper connection string")
  String zookeeper;
  
 
  /*
   -D name=value

   Define an HBase configuration option which overrides any options in
    the configuration XML files of the image or in the image configuration
     directory. The values will be persisted.
      Configuration options are only passed to the cluster when creating or reconfiguring a cluster.

   */

  @Parameter(names = "-D", description = "Definitions")
  List<String> definitions = new ArrayList<String>();
  Map<String, String> definitionMap = [:]

  @Parameter(names = "--min", description = "Minimum number of nodes")
  int min = 0;

  @Parameter(names = "--max",
      description = "Maximum number of nodes")
  int max = -1
  /**
   * fields
   */
  JCommander commander;
  String action
  List<String> actionArgs
  final String[] args

  CommonArgs(String[] args) {
    commander = new JCommander(this)
    this.args = args
  }

  String usage() {
    StringBuilder builder = new StringBuilder("\n")
    commander.usage(builder, "  ")
    builder.append("\nactions: ")
    getActions().each { key, value ->
      builder.append(key).append(" ")
    }
    return builder.toString();
  }

  void parse() {
    try {
      commander.parse(args)
    } catch (ParameterException e) {
      throw new HoyaExceptions.BadCommandArguments(e.toString()
                                                       + " with " + args.join(" ")
                                                   , e)
    }
  }

  /**
   * Map of supported actions to (description, #of args following)
   * format is of style:
   * <pre>
   *   (ACTION_CREATE): ["create cluster", 1],
   * </pre>
   * @return
   */
  Map<String, List<Object>> getActions() {
    return [:]
  }
  
/**
 * validate args via {@link #validate()}
 * then postprocess the arguments
 */
  public void postProcess() {
    validate();
    String s=""
    
    definitions.each { prop ->
      String[] keyval = ((String)prop).split("=", 2);
      if (keyval.length == 2) {
        definitionMap[keyval[0]] = keyval[1]
      }
    }
  }

  public void validate() {
    if (!parameters.size()) {
      throw new HoyaExceptions.BadCommandArguments(ERROR_NO_ACTION
                                                       + " in " + args.join(" ")
                                                       + usage())
    }
    action = parameters[0]
    log.debug("action=$action")
    Map<String, List<Object>> actionMap = getActions()
    List<Object> actionOpts = actionMap[action]
    if (!actionOpts) {
      throw new HoyaExceptions.BadCommandArguments(ERROR_UNKNOWN_ACTION
                                                       + action
          + " in " + args.join(" ")
                                                       + usage())
    }
    assert actionOpts.size() >= 2
    actionArgs = parameters.subList(1, parameters.size())

    int minArgs = (Integer)actionOpts[1]
    int actionArgSize = actionArgs.size()
    log.debug("Action $action expected #args=$minArgs actual #args=${actionArgSize}")
    if (minArgs > actionArgSize) {
      throw new HoyaExceptions.BadCommandArguments(ERROR_NOT_ENOUGH_ARGUMENTS + action
                                                       + " in " + args.join(" ")
      )
    }
    int maxArgs = (actionOpts.size() == 3) ? ((Integer)actionOpts[2]) : minArgs
    if (actionArgSize > maxArgs) {
      throw new HoyaExceptions.BadCommandArguments(ERROR_TOO_MANY_ARGUMENTS + action
                                                       + " in " + args.join(" "))
    }
  }

  public void applyDefinitions(Configuration conf) {
    definitionMap.each { key, val ->
      conf.set(key.toString(), val.toString(), "command line")
    }

  }
}
