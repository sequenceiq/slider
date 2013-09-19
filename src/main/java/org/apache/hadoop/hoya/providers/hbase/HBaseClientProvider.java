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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.HoyaKeys;
import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.api.RoleKeys;
import org.apache.hadoop.hoya.exceptions.BadConfigException;
import org.apache.hadoop.hoya.exceptions.HoyaException;
import org.apache.hadoop.hoya.providers.ClientProvider;
import org.apache.hadoop.hoya.providers.ProviderCore;
import org.apache.hadoop.hoya.providers.ProviderRole;
import org.apache.hadoop.hoya.providers.ProviderUtils;
import org.apache.hadoop.hoya.tools.ConfigHelper;
import org.apache.hadoop.hoya.tools.HoyaUtils;
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
public class HBaseClientProvider extends Configured implements
                                                          ProviderCore,
                                                          HBaseKeys, HoyaKeys,
                                                          ClientProvider {


  public static final String ERROR_UNKNOWN_ROLE = "Unknown role ";
  protected static final Logger log =
    LoggerFactory.getLogger(HBaseClientProvider.class);
  protected static final String NAME = "hbase";
  private static final ProviderUtils providerUtils = new ProviderUtils(log);

  protected HBaseClientProvider(Configuration conf) {
    super(conf);
  }

  /**
   * List of roles
   */
  protected static final List<ProviderRole> ROLES = new ArrayList<ProviderRole>();

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
  public String getName() {
    return NAME;
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
    rolemap.put(RoleKeys.YARN_CORES, Integer.toString(RoleKeys.DEF_YARN_CORES));
    rolemap.put(RoleKeys.YARN_MEMORY, Integer.toString(RoleKeys.DEF_YARN_MEMORY));
    if (rolename.equals(HBaseKeys.ROLE_WORKER)) {
      rolemap.put(RoleKeys.APP_INFOPORT, DEFAULT_HBASE_WORKER_INFOPORT);
      rolemap.put(RoleKeys.JVM_HEAP, DEFAULT_HBASE_WORKER_HEAP);
    } else if (rolename.equals(HBaseKeys.ROLE_MASTER)) {
      rolemap.put(RoleKeys.ROLE_INSTANCES, "1");
      rolemap.put(RoleKeys.APP_INFOPORT, DEFAULT_HBASE_MASTER_INFOPORT);
      rolemap.put(RoleKeys.JVM_HEAP, DEFAULT_HBASE_MASTER_HEAP);
    } else {
      throw new HoyaException(ERROR_UNKNOWN_ROLE + rolename);
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

    Map<String, String> master = clusterSpec.getMandatoryRole(
      HBaseKeys.ROLE_MASTER);

    Map<String, String> worker = clusterSpec.getMandatoryRole(
      HBaseKeys.ROLE_WORKER);

    Map<String, String> sitexml = new HashMap<String, String>();
    providerUtils.propagateSiteOptions(clusterSpec, sitexml);
    
    sitexml.put(HBaseConfigFileOptions.KEY_HBASE_CLUSTER_DISTRIBUTED, "true");
    sitexml.put(HBaseConfigFileOptions.KEY_HBASE_MASTER_PORT, "0");

    sitexml.put(HBaseConfigFileOptions.KEY_HBASE_MASTER_INFO_PORT, master.get(
      RoleKeys.APP_INFOPORT));
    sitexml.put(HBaseConfigFileOptions.KEY_HBASE_ROOTDIR,
                clusterSpec.dataPath);
    sitexml.put(HBaseConfigFileOptions.KEY_REGIONSERVER_INFO_PORT,
                worker.get(RoleKeys.APP_INFOPORT));
    sitexml.put(HBaseConfigFileOptions.KEY_REGIONSERVER_PORT, "0");
    sitexml.put(HBaseConfigFileOptions.KEY_ZNODE_PARENT, clusterSpec.zkPath);
    sitexml.put(HBaseConfigFileOptions.KEY_ZOOKEEPER_PORT,
                Integer.toString(clusterSpec.zkPort));
    sitexml.put(HBaseConfigFileOptions.KEY_ZOOKEEPER_QUORUM,
                clusterSpec.zkHosts);
    return sitexml;
  }

  /**
   * Build time review and update of the cluster specification
   * @param clusterSpec spec
   */
  @Override // Client
  public void reviewAndUpdateClusterSpec(ClusterDescription clusterSpec) throws
                                                                         HoyaException{

    validateClusterSpec(clusterSpec);
  }

  /**
   * Validate the cluster specification. This can be invoked on both
   * server and client
   * @param clusterSpec
   */
  @Override // Client and Server
  public void validateClusterSpec(ClusterDescription clusterSpec) throws
                                                                  HoyaException {
    providerUtils.validateNodeCount(HBaseKeys.ROLE_WORKER,
                                    clusterSpec.getDesiredInstanceCount(
                                      HBaseKeys.ROLE_WORKER,
                                      0), 0, -1);


    providerUtils.validateNodeCount(HoyaKeys.ROLE_MASTER,
                                    clusterSpec.getDesiredInstanceCount(
                                      HoyaKeys.ROLE_MASTER,
                                      0),
                                    0,
                                    1);
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
  @Override
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
      HBaseKeys.SITE_XML,
      HBaseKeys.HBASE_TEMPLATE_RESOURCE);

    //construct the cluster configuration values
    Map<String, String> clusterConfMap = buildSiteConfFromSpec(clusterSpec);
    //merge them
    ConfigHelper.addConfigMap(siteConf, clusterConfMap);

    if (log.isDebugEnabled()) {
      ConfigHelper.dumpConf(siteConf);
    }

    Path sitePath = ConfigHelper.generateConfig(serviceConf,
                                                siteConf,
                                                generatedConfDirPath,
                                                HBaseKeys.SITE_XML);

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
  @Override
  public void prepareAMResourceRequirements(ClusterDescription clusterSpec,
                                            Resource capability) {
    //no-op unless you want to add more memory
    capability.setMemory(clusterSpec.getRoleOptInt(
      HBaseKeys.ROLE_MASTER,
      RoleKeys.YARN_MEMORY,
      capability.getMemory()));
    capability.setVirtualCores(1);
  }


  /**
   * Any operations to the service data before launching the AM
   * @param clusterSpec cspec
   * @param serviceData map of service data
   */
  @Override  //Client
  public void prepareAMServiceData(ClusterDescription clusterSpec,
                                   Map<String, ByteBuffer> serviceData) {
    
  }


  /**
   * Get the path to hbase home
   * @return the hbase home path
   */
  public  File buildHBaseBinPath(ClusterDescription cd) {
    return new File(buildHBaseDir(cd),
                                HBaseKeys.HBASE_SCRIPT);
  }

  public  File buildHBaseDir(ClusterDescription cd) {
    String archiveSubdir = HBaseKeys.ARCHIVE_SUBDIR;
    return providerUtils.buildImageDir(cd, archiveSubdir);
  }
  
}
