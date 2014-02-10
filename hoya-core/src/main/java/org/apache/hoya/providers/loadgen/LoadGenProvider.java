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

package org.apache.hoya.providers.loadgen;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.RoleKeys;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.providers.AbstractProviderCore;
import org.apache.hoya.providers.ClientProvider;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderUtils;
import org.apache.hoya.providers.hbase.HBaseKeys;
import org.apache.hoya.tools.ConfigHelper;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
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
public class LoadGenProvider extends AbstractProviderCore implements
                                                LoadGenKeys,
                                                ClientProvider {

  protected static final Logger log =
    LoggerFactory.getLogger(LoadGenProvider.class);
  private static final ProviderUtils providerUtils = new ProviderUtils(log);

  protected LoadGenProvider(Configuration conf) {
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
    ROLES.add(new ProviderRole(ROLE_MASTER, 1));
    ROLES.add(new ProviderRole(ROLE_CPULOAD, 2));
    ROLES.add(new ProviderRole(ROLE_FAILING, 3));
    ROLES.add(new ProviderRole(ROLE_GENERAL1, 4));
    ROLES.add(new ProviderRole(ROLE_GENERAL2, 5));
    ROLES.add(new ProviderRole(ROLE_IOLOAD, 6));
  }

  @Override
  public String getName() {
    return PROVIDER_NAME;
  }


  @Override
  public Configuration create(Configuration conf) {
    return conf;
  }

  @Override
  public List<ProviderRole> getRoles() {
    return ROLES;
  }

  @Override
  public Configuration getDefaultClusterConfiguration() throws
                                                        FileNotFoundException {
    return ConfigHelper.loadMandatoryResource(
      "org/apache/hoya/providers/loadgen/loadgen.xml");
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
                                                                       HoyaException,
                                                                       FileNotFoundException {
    Map<String, String> rolemap = new HashMap<String, String>();
      Configuration conf = ConfigHelper.loadMandatoryResource(
        "org/apache/hoya/providers/loadgen/role-loadgen.xml");
      HoyaUtils.mergeEntries(rolemap, conf);
    rolemap.put(RoleKeys.ROLE_NAME, rolename);
    return rolemap;
  }

  /**
   * Build the conf dir from the service arguments, adding the hbase root
   * to the FS root dir.
   * This the configuration used by HBase directly
   * @param clusterSpec this is the cluster specification used to define this
   * @return a map of the dynamic bindings for this Hoya instance
   */
  @VisibleForTesting
  public Map<String, String> buildSiteConfFromSpec(ClusterDescription clusterSpec)
    throws BadConfigException {


    Map<String, String> sitexml = new HashMap<String, String>();

    return sitexml;
  }

  @Override
  public void reviewAndUpdateClusterSpec(ClusterDescription clusterSpec) throws
                                                                         HoyaException {
  }

  @Override //Client
  public void preflightValidateClusterConfiguration(HoyaFileSystem hoyaFileSystem,
                                                    String clustername,
                                                    Configuration configuration,
                                                    ClusterDescription clusterSpec,
                                                    Path clusterDirPath,
                                                    Path generatedConfDirPath,
                                                    boolean secure) throws
                                                                    HoyaException,
                                                                    IOException {
    validateClusterSpec(clusterSpec);
  }

  @Override
  public void validateClusterSpec(ClusterDescription clusterSpec) throws
                                                                  HoyaException {
  }

  /**
   * This builds up the site configuration for the AM and downstream services;
   * the path is added to the cluster spec so that launchers in the 
   * AM can pick it up themselves. 
   *
   *
   *
   *
   * @param hoyaFileSystem filesystem
   * @param serviceConf conf used by the service
   * @param clusterSpec cluster specification
   * @param originConfDirPath the original config dir -treat as read only
   * @param generatedConfDirPath path to place generated artifacts
   * @param clientConfExtras
   * @param libdir
   * @param tempPath
   * @return a map of name to local resource to add to the AM launcher
   */
  @Override
  public Map<String, LocalResource> prepareAMAndConfigForLaunch(HoyaFileSystem hoyaFileSystem,
                                                                Configuration serviceConf,
                                                                ClusterDescription clusterSpec,
                                                                Path originConfDirPath,
                                                                Path generatedConfDirPath,
                                                                Configuration clientConfExtras,
                                                                String libdir,
                                                                Path tempPath) throws
                                                                               IOException,
                                                                               BadConfigException {
    Configuration siteConf = ConfigHelper.loadTemplateConfiguration(
      serviceConf,
      originConfDirPath,
      HBaseKeys.SITE_XML,
      HBaseKeys.HBASE_TEMPLATE_RESOURCE);

    //construct the cluster configuration values
    Map<String, String> clusterConfMap = buildSiteConfFromSpec(
      clusterSpec);
    //merge them
    ConfigHelper.addConfigMap(siteConf, clusterConfMap.entrySet(),
                              "LoadGen provider");

    if (log.isDebugEnabled()) {
      ConfigHelper.dumpConf(siteConf);
    }

    Path sitePath = ConfigHelper.saveConfig(serviceConf,
                                            siteConf,
                                            generatedConfDirPath,
                                            HBaseKeys.SITE_XML);

    log.debug("Saving the config to {}", sitePath);
    Map<String, LocalResource> confResources;
    confResources = hoyaFileSystem.submitDirectory(
            generatedConfDirPath,
            HoyaKeys.PROPAGATED_CONF_DIR_NAME);
    return confResources;
  }

  /**
   * Update the AM resource with any local needs
   * @param capability capability to update
   */
  @Override
  public void prepareAMResourceRequirements(ClusterDescription clusterSpec,
                                            Resource capability) {

  }


  /**
   * Any operations to the service data before launching the AM
   * @param clusterSpec cspec
   * @param serviceData map of service data
   */
  @Override
  public void prepareAMServiceData(ClusterDescription clusterSpec,
                                   Map<String, ByteBuffer> serviceData) {

  }

  //  @Override
  public void buildContainerLaunchContext(ContainerLaunchContext ctx,
                                          HoyaFileSystem hoyaFileSystem,
                                          Path generatedConfPath,
                                          String role,
                                          ClusterDescription clusterSpec,
                                          Map<String, String> roleOptions
                                         ) throws IOException {
    // Set the environment
    Map<String, String> env = HoyaUtils.buildEnvMap(roleOptions);
    env.put(HBaseKeys.HBASE_LOG_DIR, providerUtils.getLogdir());

    ctx.setEnvironment(env);

    //local resources
    Map<String, LocalResource> localResources =
      new HashMap<String, LocalResource>();

    //add the configuration resources
    Map<String, LocalResource> confResources;
    confResources = hoyaFileSystem.submitDirectory(generatedConfPath, HoyaKeys.PROPAGATED_CONF_DIR_NAME);
    localResources.putAll(confResources);
    //Add binaries
    //now add the image if it was set
    if (clusterSpec.isImagePathSet()) {
      Path imagePath = new Path(clusterSpec.getImagePath());
      log.info("using image path {}", imagePath);
      hoyaFileSystem.maybeAddImagePath(localResources, imagePath);
    }
    ctx.setLocalResources(localResources);


    List<String> command = new ArrayList<String>();
    //this must stay relative if it is an image
    command.add("sleep");
    command.add("60000");
    command.add(
      "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/out.txt");
    command.add(
      "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/err.txt");

    String cmdStr = HoyaUtils.join(command, " ");

    List<String> commands = new ArrayList<String>();
    commands.add(cmdStr);
    ctx.setCommands(commands);

  }

}
