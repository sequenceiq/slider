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

package org.apache.hadoop.hoya.providers.accumulo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.HoyaKeys;
import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hadoop.hoya.exceptions.BadConfigException;
import org.apache.hadoop.hoya.exceptions.HoyaException;
import org.apache.hadoop.hoya.providers.AbstractProviderService;
import org.apache.hadoop.hoya.providers.ProviderCore;
import org.apache.hadoop.hoya.providers.ProviderRole;
import org.apache.hadoop.hoya.providers.ProviderService;
import org.apache.hadoop.hoya.providers.ProviderUtils;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.service.CompositeService;
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

public class AccumuloProviderService extends AbstractProviderService implements
                                                        ProviderCore,
                                                        AccumuloKeys {

  protected static final Logger log =
    LoggerFactory.getLogger(AccumuloProvider.class);
  private AccumuloProvider clientProvider;
  private static final ProviderUtils providerUtils = new ProviderUtils(log);

  public AccumuloProviderService() {
    super("accumulo");
  }


  @Override
  public List<ProviderRole> getRoles() {
    return AccumuloProvider.ROLES;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    clientProvider = new AccumuloProvider(conf);
  }

  @Override
  public int getDefaultMasterInfoPort() {
    return 0;
  }

  @Override
  public String getSiteXMLFilename() {
    return SITE_XML;
  }

  @Override
  public void validateClusterSpec(ClusterDescription clusterSpec) throws
                                                                  HoyaException {
    clientProvider.validateClusterSpec(clusterSpec);
  }

  /*
   ======================================================================
   Server interface below here
   ======================================================================
  */
  @Override //server
  public void buildContainerLaunchContext(ContainerLaunchContext ctx,
                                          FileSystem fs,
                                          Path generatedConfPath,
                                          String role,
                                          ClusterDescription clusterSpec,
                                          Map<String, String> roleOptions
                                         ) throws
                                           IOException,
                                           BadConfigException {
    // Set the environment
    Map<String, String> env = HoyaUtils.buildEnvMap(roleOptions);
    env.put(ACCUMULO_LOG_DIR, providerUtils.getLogdir());
    String hadoop_home =
      ApplicationConstants.Environment.HADOOP_COMMON_HOME.$();
    hadoop_home = clusterSpec.getOption(OPTION_HADOOP_HOME, hadoop_home);
    env.put(HADOOP_HOME, clusterSpec.getMandatoryOption(OPTION_HADOOP_HOME));
    env.put(HADOOP_PREFIX, hadoop_home);
    env.put(ACCUMULO_HOME,
            providerUtils.convertToAppRelativePath(
              AccumuloProvider.buildImageDir(clusterSpec)));
    env.put(ACCUMULO_CONF_DIR,
            providerUtils.convertToAppRelativePath(
              HoyaKeys.PROPAGATED_CONF_DIR_NAME));
    env.put(ZOOKEEPER_HOME, clusterSpec.getMandatoryOption(OPTION_ZK_HOME));

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

    List<String> commands = new ArrayList<String>();
    commands.add(cmd("export", HADOOP_HOME, "\"$HADOOP_HOME\""));
    commands.add(cmd("export", ZOOKEEPER_HOME, "\"$ZOOKEEPER_HOME\""));


    List<String> command = new ArrayList<String>();
    //this must stay relative if it is an image
    command.add(AccumuloProvider.buildScriptBinPath(clusterSpec).toString());

    //config dir is relative to the generated file
    command.add(HoyaKeys.PROPAGATED_CONF_DIR_NAME);
    //role is region server
    command.add(role);

    //log details
    command.add(
      "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/out.txt");
    command.add(
      "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/err.txt");

    String cmdStr = HoyaUtils.join(command, " ");


    commands.add(cmdStr);
    ctx.setCommands(commands);

  }

  /**
   * build up the in-container master comand
   *
   * @param clusterSpec
   * @param confDir
   * @param env
   * @param masterCommand
   * @return
   * @throws IOException
   * @throws HoyaException
   */
  @Override //server
  public List<String> buildProcessCommand(ClusterDescription clusterSpec,
                                          File confDir,
                                          Map<String, String> env,
                                          String masterCommand) throws
                                                                IOException,
                                                                HoyaException {
//    env.put(HBaseKeys.HBASE_LOG_DIR, new ProviderUtils(log).getLogdir());
    env.put(ACCUMULO_LOG_DIR, providerUtils.getLogdir());
    String hadoop_home = System.getenv(HADOOP_HOME);
    hadoop_home = clusterSpec.getOption(OPTION_HADOOP_HOME, hadoop_home);
    if (hadoop_home == null) {
      throw new BadConfigException(
        "Undefined env variable/config option: " + HADOOP_HOME);
    }
    env.put(HADOOP_HOME, hadoop_home);
    env.put(HADOOP_PREFIX, hadoop_home);
    File image = AccumuloProvider.buildImageDir(clusterSpec);
    File dot = new File(".");
    env.put(ACCUMULO_HOME, image.getAbsolutePath());
    env.put(ACCUMULO_CONF_DIR,
            new File(dot, HoyaKeys.PROPAGATED_CONF_DIR_NAME).getAbsolutePath());
    env.put(ZOOKEEPER_HOME, clusterSpec.getMandatoryOption(OPTION_ZK_HOME));

    //set the service to run if unset
    if (masterCommand == null) {
      masterCommand = AccumuloKeys.CREATE_MASTER;
    }
    //prepend the hbase command itself
    File binScriptSh = AccumuloProvider.buildScriptBinPath(clusterSpec);
    String scriptPath = binScriptSh.getAbsolutePath();
    if (!binScriptSh.exists()) {
      throw new BadCommandArgumentsException("Missing script " + scriptPath);
    }
    List<String> launchSequence = new ArrayList<String>(8);
    launchSequence.add(0, scriptPath);
    launchSequence.add(masterCommand);
    return launchSequence;
  }


  @Override
  public Map<String, String> buildSiteConfFromSpec(ClusterDescription clusterSpec) throws
                                                                                   BadConfigException {
    return clientProvider.buildSiteConfFromSpec(clusterSpec);
  }
}
