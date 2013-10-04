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

package org.apache.hadoop.hoya.providers.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.HoyaKeys;
import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hadoop.hoya.exceptions.HoyaException;
import org.apache.hadoop.hoya.providers.AbstractProviderService;
import org.apache.hadoop.hoya.providers.ProviderCore;
import org.apache.hadoop.hoya.providers.ProviderRole;
import org.apache.hadoop.hoya.providers.ProviderUtils;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.hoya.yarn.service.CompoundService;
import org.apache.hadoop.hoya.yarn.service.EventCallback;
import org.apache.hadoop.hoya.yarn.service.EventNotifyingService;
import org.apache.hadoop.hoya.yarn.service.ForkedProcessService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class implements both the client-side and server-side aspects
 * of an HBase Cluster
 */
public class HBaseProviderService extends AbstractProviderService implements
                                                                  ProviderCore,
                                                                  HBaseKeys,
                                                                  HoyaKeys {


  public static final String ERROR_UNKNOWN_ROLE = "Unknown role ";
  protected static final Logger log =
    LoggerFactory.getLogger(HBaseProviderService.class);
  protected static final String NAME = "hbase";
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  private HBaseClientProvider clientProvider;

  public HBaseProviderService() {
    super("HBaseProviderService");
  }

  /**
   * List of roles
   */
  protected static final List<ProviderRole> ROLES =
    new ArrayList<ProviderRole>();

  public static final int KEY_WORKER = 1;

  public static final int KEY_MASTER = 2;

  /**
   * Initialize role list
   */
  static {
    ROLES.add(new ProviderRole(HBaseKeys.ROLE_WORKER, KEY_WORKER));
    ROLES.add(new ProviderRole(HBaseKeys.ROLE_MASTER, KEY_MASTER, true));
  }


  @Override
  public List<ProviderRole> getRoles() {
    return ROLES;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    clientProvider = new HBaseClientProvider(conf);
  }

  @Override
  public int getDefaultMasterInfoPort() {
    return HBaseConfigFileOptions.DEFAULT_MASTER_INFO_PORT;
  }

  @Override
  public String getSiteXMLFilename() {
    return SITE_XML;
  }


  /**
   * Validate the cluster specification. This can be invoked on both
   * server and client
   * @param clusterSpec
   */
  @Override // Client and Server
  public void validateClusterSpec(ClusterDescription clusterSpec) throws
                                                                  HoyaException {
    clientProvider.validateClusterSpec(clusterSpec);
  }


  @Override  // server
  public void buildContainerLaunchContext(ContainerLaunchContext ctx,
                                          FileSystem fs,
                                          Path generatedConfPath,
                                          String role,
                                          ClusterDescription clusterSpec,
                                          Map<String, String> roleOptions
                                         ) throws IOException {
    // Set the environment
    Map<String, String> env = HoyaUtils.buildEnvMap(roleOptions);
    env.put(HBASE_LOG_DIR, providerUtils.getLogdir());

    ctx.setEnvironment(env);

    //local resources
    Map<String, LocalResource> localResources =
      new HashMap<String, LocalResource>();

    //add the configuration resources
    Map<String, LocalResource> confResources;
    confResources = HoyaUtils.submitDirectory(fs,
                                              generatedConfPath,
                                              HoyaKeys.PROPAGATED_CONF_DIR_NAME);
    localResources.putAll(confResources);
    //Add binaries
    //now add the image if it was set
    if (clusterSpec.imagePath != null) {
      Path imagePath = new Path(clusterSpec.imagePath);
      log.info("using image path {}", imagePath);
      HoyaUtils.maybeAddImagePath(fs, localResources, imagePath);
    }
    ctx.setLocalResources(localResources);


    List<String> command = new ArrayList<String>();
    //this must stay relative if it is an image
    command.add(clientProvider.buildHBaseBinPath(clusterSpec).toString());

    //config dir is relative to the generated file
    command.add(HBaseKeys.ARG_CONFIG);
    command.add(HoyaKeys.PROPAGATED_CONF_DIR_NAME);
    //role is region server
    command.add(HBaseKeys.REGION_SERVER);
    command.add(HBaseKeys.ACTION_START);
/*    command.add("-D httpfs.log.dir = "+
                ApplicationConstants.LOG_DIR_EXPANSION_VAR);*/

    //log details
    command.add(
      "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/out.txt");
    command.add(
      "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/err.txt");

    String cmdStr = HoyaUtils.join(command, " ");

    List<String> commands = new ArrayList<String>();
    commands.add(cmdStr);
    ctx.setCommands(commands);

  }

  @Override
  public List<String> buildProcessCommand(ClusterDescription cd,
                                          File confDir,
                                          Map<String, String> env,
                                          String masterCommand) throws
                                                                IOException,
                                                                HoyaException {
    env.put(HBASE_LOG_DIR, new ProviderUtils(log).getLogdir());
    //pull out the command line argument if set
    //set the service to run if unset
    if (masterCommand == null) {
      masterCommand = MASTER;
    }
    //prepend the hbase command itself
    File binHbaseSh = clientProvider.buildHBaseBinPath(cd);
    String scriptPath = binHbaseSh.getAbsolutePath();
    if (!binHbaseSh.exists()) {
      throw new BadCommandArgumentsException("Missing script " + scriptPath);
    }
    List<String> launchSequence = new ArrayList<String>(8);
    launchSequence.add(0, scriptPath);
    launchSequence.add(ARG_CONFIG);
    launchSequence.add(confDir.getAbsolutePath());
    launchSequence.add(masterCommand);
    launchSequence.add(ACTION_START);
    return launchSequence;
  }

  /**
   * Run this service
   *
   * @param cd component description
   * @param confDir local dir with the config
   * @param env environment variables above those generated by
   * @param execInProgress callback for the event notification
   * @throws IOException IO problems
   * @throws HoyaException anything internal
   */
  @Override
  public void exec(ClusterDescription cd,
                   File confDir,
                   Map<String, String> env,
                   EventCallback execInProgress) throws
                                                 IOException,
                                                 HoyaException {

    String masterCommand =
      cd.getOption(HoyaKeys.OPTION_HOYA_MASTER_COMMAND, null);

    List<String> commands =
      buildProcessCommand(cd, confDir, env, masterCommand);

    ForkedProcessService masterProcess = buildProcess(getName(), env, commands);
    CompoundService composite = new CompoundService(getName());
    composite.addService(masterProcess);
    composite.addService(new EventNotifyingService(execInProgress,
                           cd.getOptionInt(OPTION_CONTAINER_STARTUP_DELAY,
                                           CONTAINER_STARTUP_DELAY)));
    //register the service for lifecycle management; when this service
    //is terminated, so is the master process
    addService(composite);
    maybeStartCommandSequence();


  }

}
