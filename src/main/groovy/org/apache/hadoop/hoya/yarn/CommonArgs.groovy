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
import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException

/**
 * This class contains the common argument set for all tne entry points,
 * and the core parsing logic to verify that the action is on the list
 * of allowed actions -and that the remaining number of arguments is
 * in the range allowed
 */
@Commons

public class CommonArgs {
  public static final String ARG_ACTION = '--action'
  public static final String ARG_CONFDIR = '--confdir'
  public static final String ARG_DEBUG = '--debug'
  public static final String ARG_FILESYSTEM = '--filesystem'
  public static final String ARG_HBASE_HOME = '--hbasehome'
  public static final String ARG_HBASE_ZKPATH = '--hbasezkpath'
  public static final String ARG_IMAGE = '--image'
  public static final String ARG_MAX = '--max'
  public static final String ARG_MIN = '--min'
  public static final String ARG_NAME = '--name'
  public static final String ARG_PATH = '--path'
  public static final String ARG_USER = '--user'

  public static final String ARG_ZOOKEEPER = '--zookeeper'

  /** for testing only: {@value} */
  public static final String ARG_X_HBASE_COMMAND = '--Xhbase-command'
  public static final String ARG_X_NO_MASTER = '--Xnomaster'
  public static final String ARG_X_TEST = '--Xtest'


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
  public static final String ACTION_ADDNODE = "addnode"
  public static final String ACTION_CREATE = "create"
  public static final String ACTION_GETSIZE = "getsize"
  public static final String ACTION_HELP = "help"
  public static final String ACTION_EXISTS = "exists"
  public static final String ACTION_LIST = "list"
  public static final String ACTION_MIGRATE = "migrate"
  public static final String ACTION_PREFLIGHT = "preflight"
  public static final String ACTION_RECONFIGURE = "reconfigure"
  public static final String ACTION_REIMAGE = "reimage"
  public static final String ACTION_RMNODE = "rmnode"
  public static final String ACTION_START = "start"
  public static final String ACTION_STATUS = "status"
  public static final String ACTION_STOP = "stop"

  @Parameter
  public List<String> parameters = new ArrayList<String>();

  @Parameter(names = '--debug', description = "Debug mode")
  public boolean debug = false;

  @Parameter(names = '--help', help = true)
  public boolean help;
  
  @Parameter(names = "--hbasehome",
      description = "HBase home dir for starting pre-installed binaries")
  public String hbasehome;

  @Parameter(names = "--hbasezkpath",
      description = "HBase Zookeeper path")
  public String hbasezkpath;


  @Parameter(names = '--Xtest', description = "Test mode")
  public boolean xTest = false;

  @Parameter(names = "--user",
      description = "username if not self")
  public String user = System.getProperty("user.name");
  
  @Parameter(names = "--zookeeper",
      description = "Zookeeper connection string")
  public String zookeeper;
  
 
  /*
   -D name=value

   Define an HBase configuration option which overrides any options in
    the configuration XML files of the image or in the image configuration
     directory. The values will be persisted.
      Configuration options are only passed to the cluster when creating or reconfiguring a cluster.

   */

  @Parameter(names = "-D", description = "Definitions")
  public List<String> definitions = new ArrayList<String>();
  public Map<String, String> definitionMap = [:]

  @Parameter(names = "--min", description = "Minimum number of nodes")
  public int min = 0;

  @Parameter(names = "--max",
      description = "Maximum number of nodes")
  public int max = -1


  @Parameter(names = "--Xhbase-command",
      description = "testing only: hbase command to exec")
  public String hbaseCommand="master";

  @Parameter(names = '--Xnomaster',
      description = "testing only: no master at all")
  public boolean xNoMaster = false;

  
  
  /**
   * fields
   */
  JCommander commander;
  String action
  List<String> actionArgs
  final String[] args
  //
  /**
   * get the name: relies on arg 1 being the cluster name in all operations 
   * @return the name argument, null if there is none
   */
  String getName() {
    return actionArgs.isEmpty()? null: args[0] 
  }

  public CommonArgs(String[] args) {
    this.args = args
    commander = new JCommander(this)
  }

  public String usage() {
    StringBuilder builder = new StringBuilder("\n")
    commander.usage(builder, "  ")
    builder.append("\nactions: ")
    getActions().each { key, value ->
      builder.append(key).append(" ")
    }
    return builder.toString();
  }

  public void parse() {
    try {
      commander.parse(args)
    } catch (ParameterException e) {
      throw new BadCommandArgumentsException(e.toString()
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
  public Map<String, List<Object>> getActions() {
    return [:]
  }
  
/**
 * validate args via {@link #validate()}
 * then postprocess the arguments
 */
  public void postProcess() {
    validate();
    
    definitions.each { prop ->
      String[] keyval = ((String)prop).split("=", 2);
      if (keyval.length == 2) {
        definitionMap[keyval[0]] = keyval[1]
      }
    }
  }

  public void validate() {
    if (!parameters.size()) {
      throw new BadCommandArgumentsException(ERROR_NO_ACTION
                                                       + " in " + args.join(" ")
                                                       + usage())
    }
    action = parameters[0]
    log.debug("action=$action")
    Map<String, List<Object>> actionMap = getActions()
    List<Object> actionOpts = actionMap[action]
    if (!actionOpts) {
      throw new BadCommandArgumentsException(ERROR_UNKNOWN_ACTION
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
      throw new BadCommandArgumentsException(ERROR_NOT_ENOUGH_ARGUMENTS + action
                                                       + " in " + args.join(" ")
      )
    }
    int maxArgs = (actionOpts.size() == 3) ? ((Integer)actionOpts[2]) : minArgs
    if (actionArgSize > maxArgs) {
      throw new BadCommandArgumentsException(ERROR_TOO_MANY_ARGUMENTS + action
                                                       + " in " + args.join(" "))
    }
  }

  public void applyDefinitions(Configuration conf) {
    definitionMap.each { key, val ->
      conf.set(key.toString(), val.toString(), "command line")
    }

  }
}
