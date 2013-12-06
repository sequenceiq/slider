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

package org.apache.hadoop.hoya.yarn;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hadoop.hoya.exceptions.ErrorStrings;
import org.apache.hadoop.hoya.providers.hbase.HBaseConfigFileOptions;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.hoya.yarn.params.PathArgumentConverter;
import org.apache.hadoop.hoya.yarn.params.URIArgumentConverter;
import org.apache.hadoop.hoya.yarn.params.ArgOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains the common argument set for all tne entry points,
 * and the core parsing logic to verify that the action is on the list
 * of allowed actions -and that the remaining number of arguments is
 * in the range allowed
 */

public class CommonArgs extends ArgOps implements HoyaActions, Arguments {

  protected static final Logger log = LoggerFactory.getLogger(CommonArgs.class);

  /**
   * This is the default parameter
   */
  @Parameter
  public List<String> parameters = new ArrayList<String>();

  @Parameter(names = ARG_DEBUG, description = "Debug mode")
  private boolean debug = false;

  @Parameter(names = ARG_CONFDIR,
             description = "Path to cluster configuration directory in HDFS",
             converter = PathArgumentConverter.class)
  private Path confdir;

  @Parameter(names = {ARG_FILESYSTEM, ARG_FILESYSTEM_LONG}, description = "Filesystem URI",
             converter = URIArgumentConverter.class)
  private URI filesystemURL;

  @Parameter(names = ARG_APP_ZKPATH,
             description = "Zookeeper path for the application")
  private String appZKPath;

  @Parameter(names = ARG_HELP, help = true)
  public boolean help;


  @Parameter(names = {"--m", ARG_MANAGER},
             description = "hostname:port of the YARN resource manager")
  private String manager;

  //TODO: do we need this?
  @Parameter(names = ARG_RESOURCE_MANAGER,
             description = "Resource manager hostname:port ",
             required = false)
  private String rmAddress;

  @Parameter(names = ARG_ZKHOSTS,
             description = "comma separated list of the Zookeeper hosts")
  private String zkhosts;
  
  @Parameter(names = ARG_ZKPORT,
             description = "Zookeeper port")
  private int zkport = HBaseConfigFileOptions.HBASE_ZK_PORT;

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

  @Parameter(names = {ARG_OUTPUT, "-o"},
             description = "Output file for the configuration data")
  private String output;

  /**
   * fields
   */
  public JCommander commander;
  private String action;
  //action arguments; 
  private List<String> actionArgs;
  private final String[] args;

  /**
   * get the name: relies on arg 1 being the cluster name in all operations 
   * @return the name argument, null if there is none
   */
  public String getClusterName() {
    return (getActionArgs() == null || getActionArgs().isEmpty() || getArgs().length < 2) ?
           null : getArgs()[1];
  }

  public CommonArgs(String[] args) {
    this.args = args;
    commander = new JCommander(this);
  }

  public CommonArgs(Collection args) {
    List<String> argsAsStrings = HoyaUtils.collectionToStringList(args);
    this.args = argsAsStrings.toArray(new String[argsAsStrings.size()]);
    commander = new JCommander(this);
  }


  public String usage() {
    StringBuilder builder = new StringBuilder("\n");
    commander.usage(builder, "  ");
    builder.append("\nactions: ");
    Map<String, List<Object>> actions = getActions();
    List<String>keys = new ArrayList<String>(actions.keySet());
    Collections.sort(keys);
    for (String key : keys) {
      builder.append(key).append(" ");
    }
    return builder.toString();
  }

  public void parse() throws BadCommandArgumentsException {
    try {
      commander.parse(getArgs());
    } catch (ParameterException e) {
      throw new BadCommandArgumentsException(e, "%s in %s", 
        e.toString(),
        (getArgs() != null ? ( HoyaUtils.join(getArgs(), " ")) : "[]"));
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
    return Collections.emptyMap();
  }

  /**
   * validate args via {@link #validate()}
   * then postprocess the arguments
   */
  public void postProcess() throws BadCommandArgumentsException {
    validate();
    splitPairs(definitions, definitionMap);
    splitPairs(sysprops, syspropsMap);
    for (Map.Entry<String, String> entry : syspropsMap.entrySet()) {
      System.setProperty(entry.getKey(),entry.getValue());
    }
  }

  /**
   * Validate the arguments against the action requested
   */
  public void validate() throws BadCommandArgumentsException {
    if (parameters.isEmpty()) {
      throw new BadCommandArgumentsException(ErrorStrings.ERROR_NO_ACTION
                                             + usage());
    }
    setAction(parameters.get(0));
    log.debug("action={}", getAction());
    Map<String, List<Object>> actionMap = getActions();
    List<Object> actionOpts = actionMap.get(getAction());
    if (null == actionOpts) {
      throw new BadCommandArgumentsException(ErrorStrings.ERROR_UNKNOWN_ACTION
                                             + getAction()
                                             + usage());
    }
    setActionArgs(parameters.subList(1, parameters.size()));

    int minArgs = (Integer) actionOpts.get(1);
    int actionArgSize = getActionArgs().size();
    if (minArgs > actionArgSize) {
      throw new BadCommandArgumentsException(
        ErrorStrings.ERROR_NOT_ENOUGH_ARGUMENTS + getAction());
    }
    int maxArgs =
      (actionOpts.size() == 3) ? ((Integer) actionOpts.get(2)) : minArgs;
    if (actionArgSize > maxArgs) {
      String message = String.format("%s for %s: limit is %d but saw %d",
                                     ErrorStrings.ERROR_TOO_MANY_ARGUMENTS,
                                     getAction(), maxArgs,
                                     actionArgSize);
      log.error(message);
      int index=1;
      for (String actionArg : getActionArgs()) {
        log.error("[{}] \"{}\"", index++, actionArg);
      }
      throw new BadCommandArgumentsException(message);
    }
  }

  /**
   * Apply all the definitions on the command line to the configuration
   * @param conf config
   */
  public void applyDefinitions(Configuration conf) throws
                                                   BadCommandArgumentsException {
    applyDefinitions(definitionMap, conf);
  }


  /**
   * If the Filesystem URL was provided, it overrides anything in
   * the configuration
   * @param conf configuration
   */
  public void applyFileSystemURL(Configuration conf) {
    ArgOps.applyFileSystemURL(getFilesystemURL(), conf);
  }

  public boolean isDebug() {
    return debug;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  /**
   *    Declare the image configuration directory to use when creating or reconfiguring a hoya cluster. The path must be on a filesystem visible to all nodes in the YARN cluster.
   Only one configuration directory can be specified.
   */
  public Path getConfdir() {
    return confdir;
  }

  public void setConfdir(Path confdir) {
    this.confdir = confdir;
  }

  public URI getFilesystemURL() {
    return filesystemURL;
  }

  public void setFilesystemURL(URI filesystemURL) {
    this.filesystemURL = filesystemURL;
  }

  public String getAppZKPath() {
    return appZKPath;
  }

  public void setAppZKPath(String appZKPath) {
    this.appZKPath = appZKPath;
  }

  public String getManager() {
    return manager;
  }

  public void setManager(String manager) {
    this.manager = manager;
  }

  public String getRmAddress() {
    return rmAddress;
  }

  public void setRmAddress(String rmAddress) {
    this.rmAddress = rmAddress;
  }

  public String getZkhosts() {
    return zkhosts;
  }

  public void setZkhosts(String zkhosts) {
    this.zkhosts = zkhosts;
  }

  public int getZkport() {
    return zkport;
  }

  public void setZkport(int zkport) {
    this.zkport = zkport;
  }

  public String getOutput() {
    return output;
  }

  public void setOutput(String output) {
    this.output = output;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public List<String> getActionArgs() {
    return actionArgs;
  }

  public void setActionArgs(List<String> actionArgs) {
    this.actionArgs = actionArgs;
  }

  public String[] getArgs() {
    return args;
  }
}
