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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.HoyaKeys;
import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.api.RoleKeys;
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hadoop.hoya.exceptions.BadConfigException;
import org.apache.hadoop.hoya.exceptions.HoyaException;
import org.apache.hadoop.hoya.providers.ClientProvider;
import org.apache.hadoop.hoya.providers.ProviderCore;
import org.apache.hadoop.hoya.providers.ProviderRole;
import org.apache.hadoop.hoya.providers.ProviderUtils;
import org.apache.hadoop.hoya.providers.ServerProvider;
import org.apache.hadoop.hoya.tools.ConfigHelper;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class implements both the client-side and server-side aspects
 * of an HBase Cluster
 */
public class AccumuloProvider extends Configured implements
                                                 ProviderCore,
                                                 AccumuloKeys,
                                                 ClientProvider,
                                                 ServerProvider {

  protected static final Logger log =
    LoggerFactory.getLogger(AccumuloProvider.class);
  private static final ProviderUtils providerUtils = new ProviderUtils(log);

  protected AccumuloProvider(Configuration conf) {
    super(conf);
  }

  /**
   * List of roles
   */
  protected static final List<ProviderRole> ROLES =
    new ArrayList<ProviderRole>();

  /**
   * Initialize role list
   */
  static {
    ROLES.add(new ProviderRole(ROLE_MASTER, 1, true));
    ROLES.add(new ProviderRole(ROLE_TABLET, 2));
    ROLES.add(new ProviderRole(ROLE_GARBAGE_COLLECTOR, 3));
    ROLES.add(new ProviderRole(ROLE_MONITOR, 4));
    ROLES.add(new ProviderRole(ROLE_TRACER, 5));
  }

  @Override
  public String getName() {
    return PROVIDER_ACCUMULO;
  }

  @Override
  public List<ProviderRole> getRoles() {
    return ROLES;
  }


  /**
   * Get a map of all the default options for the cluster; values
   * that can be overridden by user defaults after
   * @return a possibly emtpy map of default cluster options.
   */
  @Override
  public Map<String, String> getDefaultClusterOptions() {
    return new HashMap<String, String>();
  }

  /**
   * Create the default cluster role instance for a named
   * cluster role; 
   *
   * @param rolename role name
   * @return a node that can be added to the JSON
   */
  @Override
  public Map<String, String> createDefaultClusterRole(String rolename) throws
                                                                       HoyaException {
    Map<String, String> rolemap = new HashMap<String, String>();
    rolemap.put(RoleKeys.ROLE_NAME, rolename);

    rolemap.put(RoleKeys.JVM_HEAP, DEFAULT_ROLE_HEAP);
    rolemap.put(RoleKeys.YARN_CORES, DEFAULT_ROLE_YARN_VCORES);
    rolemap.put(RoleKeys.YARN_MEMORY, DEFAULT_ROLE_YARN_RAM);

    if (rolename.equals(ROLE_MASTER)) {
      rolemap.put(RoleKeys.JVM_HEAP, DEFAULT_MASTER_HEAP);
      rolemap.put(RoleKeys.YARN_CORES, DEFAULT_MASTER_YARN_VCORES);
      rolemap.put(RoleKeys.YARN_MEMORY, DEFAULT_MASTER_YARN_RAM);

    } else if (rolename.equals(ROLE_TABLET)) {
    } else if (rolename.equals(ROLE_TRACER)) {
    } else if (rolename.equals(ROLE_GARBAGE_COLLECTOR)) {
    } else if (rolename.equals(ROLE_MONITOR)) {
    }
    return rolemap;
  }

  /**
   * Build the conf dir from the service arguments, adding the hbase root
   * to the FS root dir.
   * This the configuration used by HBase directly
   * @param clusterSpec this is the cluster specification used to define this
   * @return a map of the dynamic bindings for this Hoya instance
   */
  public Map<String, String> buildSiteConfFromSpec(ClusterDescription clusterSpec)
    throws BadConfigException {


    Map<String, String> sitexml = new HashMap<String, String>();

    return sitexml;
  }

  @Override
  public int getDefaultMasterInfoPort() {
    return 0;
  }

  @Override
  public String getSiteXMLFilename() {
    return SITE_XML;
  }


  /**
   * Build time review and update of the cluster specification
   * @param clusterSpec spec
   */
  @Override // Client
  public void reviewAndUpdateClusterSpec(ClusterDescription clusterSpec) throws
                                                                         HoyaException {

    validateClusterSpec(clusterSpec);
  }


  /**
   * This builds up the site configuration for the AM and downstream services;
   * the path is added to the cluster spec so that launchers in the 
   * AM can pick it up themselves. 
   * @param clusterFS filesystem
   * @param serviceConf conf used by the service
   * @param clusterSpec cluster specification
   * @param originConfDirPath the original config dir -treat as read only
   * @param generatedConfDirPath path to place generated artifacts
   * @return a map of name to local resource to add to the AM launcher
   */
  @Override //client
  public Map<String, LocalResource> prepareAMAndConfigForLaunch(FileSystem clusterFS,
                                                                Configuration serviceConf,
                                                                ClusterDescription clusterSpec,
                                                                Path originConfDirPath,
                                                                Path generatedConfDirPath) throws
                                                                                           IOException,
                                                                                           BadConfigException {
    Configuration siteConf = ConfigHelper.loadTemplateConfiguration(
      serviceConf,
      originConfDirPath,
      AccumuloKeys.SITE_XML,
      AccumuloKeys.SITE_XML_RESOURCE);

    //construct the cluster configuration values
    Map<String, String> clusterConfMap = buildSiteConfFromSpec(
      clusterSpec);
    //merge them
    ConfigHelper.addConfigMap(siteConf, clusterConfMap);

    if (log.isDebugEnabled()) {
      ConfigHelper.dumpConf(siteConf);
    }

    Path sitePath = ConfigHelper.generateConfig(serviceConf,
                                                siteConf,
                                                generatedConfDirPath,
                                                AccumuloKeys.SITE_XML);

    log.debug("Saving the config to {}", sitePath);
    Map<String, LocalResource> confResources;
    confResources = HoyaUtils.submitDirectory(clusterFS,
                                              generatedConfDirPath,
                                              HoyaKeys.PROPAGATED_CONF_DIR_NAME);
    return confResources;
  }

  /**
   * Update the AM resource with any local needs
   * @param capability capability to update
   */
  @Override //client
  public void prepareAMResourceRequirements(ClusterDescription clusterSpec,
                                            Resource capability) {
    //no-op unless you want to add more memory
    capability.setMemory(clusterSpec.getRoleOptInt(ROLE_MASTER,
                                                   RoleKeys.YARN_MEMORY,
                                                   capability.getMemory()));
    capability.setVirtualCores(1);
  }


  /**
   * Any operations to the service data before launching the AM
   * @param clusterSpec cspec
   * @param serviceData map of service data
   */
  @Override //client
  public void prepareAMServiceData(ClusterDescription clusterSpec,
                                   Map<String, ByteBuffer> serviceData) {

  }

   /*
   ======================================================================
   Client and Server interface below here
   ======================================================================
  */


  /**
   * Validate the cluster specification. This can be invoked on both
   * server and client
   * @param clusterSpec
   */
  @Override // Client and Server
  public void validateClusterSpec(ClusterDescription clusterSpec) throws
                                                                  HoyaException {
    providerUtils.validateNodeCount(AccumuloKeys.ROLE_TABLET,
                                    clusterSpec.getDesiredInstanceCount(
                                      AccumuloKeys.ROLE_TABLET,
                                      0), 0, -1);


    providerUtils.validateNodeCount(HoyaKeys.ROLE_MASTER,
                                    clusterSpec.getDesiredInstanceCount(
                                      HoyaKeys.ROLE_MASTER,
                                      0),
                                    0,
                                    1);
    clusterSpec.verifyOptionSet(AccumuloKeys.OPTION_ZK_HOME);
    clusterSpec.verifyOptionSet(AccumuloKeys.OPTION_HADOOP_HOME);
  }

  private String cmd(Object ...args) {
    List<String> list = new ArrayList<String>(args.length);
    for (Object arg : args) {
      list.add(arg.toString());
    }
    return HoyaUtils.join(list, " ");
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
                                         ) throws IOException {
    // Set the environment
    Map<String, String> env = HoyaUtils.buildEnvMap(roleOptions);
    env.put(ACCUMULO_LOG_DIR, providerUtils.getLogdir());
    String hadoop_home = ApplicationConstants.Environment.HADOOP_COMMON_HOME.$();
    hadoop_home = clusterSpec.getOption(HADOOP_HOME, hadoop_home);
    env.put(HADOOP_HOME, hadoop_home);
    env.put(HADOOP_PREFIX, hadoop_home);
    env.put(ACCUMULO_HOME,
            convertToAppRelativePath(buildImageDir(clusterSpec)));
    env.put(ACCUMULO_CONF_DIR,
            convertToAppRelativePath(HoyaKeys.PROPAGATED_CONF_DIR_NAME));
    env.put(ZOOKEEPER_HOME,clusterSpec.getOption(OPTION_ZK_HOME,""));

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
    commands.add(cmd("export",HADOOP_HOME,"\"$HADOOP_HOME\""));
    commands.add(cmd("export", ZOOKEEPER_HOME,"\"$ZOOKEEPER_HOME\""));
    
    
    List<String> command = new ArrayList<String>();
    //this must stay relative if it is an image
    command.add(buildScriptBinPath(clusterSpec).toString());

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
   * Get the path to the script
   * @return the script
   */
  public static File buildScriptBinPath(ClusterDescription cd) {
    return new File(buildImageDir(cd), AccumuloKeys.START_SCRIPT);
  }

  public String convertToAppRelativePath(File file) {
    return convertToAppRelativePath(file.getPath());
  }

  private String convertToAppRelativePath(String path) {
    return ApplicationConstants.Environment.HOME.$() + "/" + path;
  }

  /**
   * Build the image dir. This path is relative and only valid at the far end
   * @param cd cluster spec
   * @return a relative path to accumulp home
   */
  public static File buildImageDir(ClusterDescription cd) {
    File basedir;
    if (cd.imagePath != null) {
      basedir = new File(new File(HoyaKeys.LOCAL_TARBALL_INSTALL_SUBDIR),
                         AccumuloKeys.ARCHIVE_SUBDIR);
    } else {
      basedir = new File(cd.applicationHome);
    }
    return basedir;
  }

  @Override
  public List<String> buildProcessCommand(ClusterDescription cd,
                                          File confDir,
                                          Map<String, String> env) throws
                                                                   IOException,
                                                                   HoyaException {
//    env.put(HBaseKeys.HBASE_LOG_DIR, new ProviderUtils(log).getLogdir());
    //pull out the command line argument if set
    String masterCommand =
      cd.getOption(
        HoyaKeys.OPTION_HOYA_MASTER_COMMAND,
        AccumuloKeys.CREATE_MASTER);
    List<String> launchSequence = new ArrayList<String>(8);
    //prepend the hbase command itself
    File binScriptSh = buildScriptBinPath(cd);
    String scriptPath = binScriptSh.getAbsolutePath();
    if (!binScriptSh.exists()) {
      throw new BadCommandArgumentsException("Missing script " + scriptPath);
    }
    launchSequence.add(0, scriptPath);
/* todo
    launchSequence.add(HBaseKeys.ARG_CONFIG);
    launchSequence.add(confDir.getAbsolutePath());
*/
    launchSequence.add(masterCommand);
    launchSequence.add(AccumuloKeys.ACTION_START);
    return launchSequence;
  }


}
