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

package org.apache.hadoop.hoya.providers.hoyaam;

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
import org.apache.hadoop.hoya.servicemonitor.Probe;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * handles the setup of the Hoya AM.
 * This keeps aspects of role, cluster validation and Clusterspec setup
 * out of the core hoya client
 */
public class HoyaAMClientProvider extends Configured implements
                                                     ProviderCore,
                                                     HoyaKeys,
                                                     ClientProvider {


  public static final String ERROR_UNKNOWN_ROLE = "Unknown role ";
  protected static final Logger log =
    LoggerFactory.getLogger(HoyaAMClientProvider.class);
  protected static final String NAME = "hoyaAM";
  private static final ProviderUtils providerUtils = new ProviderUtils(log);

  public HoyaAMClientProvider(Configuration conf) {
    super(conf);
  }

  /**
   * List of roles
   */
  protected static final List<ProviderRole> ROLES =
    new ArrayList<ProviderRole>();

  public static final int KEY_AM = ROLE_HOYA_AM_PRIORITY_INDEX;

  /**
   * Initialize role list
   */
  static {
    ROLES.add(new ProviderRole(ROLE_HOYA_AM, KEY_AM, false));
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<Probe> createProbes(String urlStr,
                                  Configuration config,
                                  int timeout)
    throws IOException {
    List<Probe> probes = new ArrayList<Probe>();
    return probes;
  }

  @Override
  public List<ProviderRole> getRoles() {
    return ROLES;
  }


  /**
   * Get a map of all the default options for the cluster; values
   * that can be overridden by user defaults after
   * @return a possibly empty map of default cluster options.
   */
  @Override
  public Map<String, String> getDefaultClusterOptions() {
    HashMap<String, String> site = new HashMap<String, String>();
    return site;
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
    if (rolename.equals(ROLE_HOYA_AM)) {
      rolemap.put(RoleKeys.ROLE_NAME, rolename);
      rolemap.put(RoleKeys.YARN_MEMORY,
                  Integer.toString(RoleKeys.DEFAULT_AM_MEMORY));
      rolemap.put(RoleKeys.YARN_CORES,
                  Integer.toString(RoleKeys.DEFAULT_AM_V_CORES));
      rolemap.put(RoleKeys.JVM_HEAP, RoleKeys.DEFAULT_AM_HEAP);
      rolemap.put(RoleKeys.ROLE_INSTANCES, "1");
    }
    return rolemap;
  }

  /**
   * This shouldn't be used, but is here as the API requires it.
   * @param clusterSpec this is the cluster specification used to define this
   * @return a map of the dynamic bindings for this Hoya instance
   */
  public Map<String, String> buildSiteConfFromSpec(ClusterDescription clusterSpec)
    throws BadConfigException {

    Map<String, String> sitexml = new HashMap<String, String>();

    //map all cluster-wide site. options
    providerUtils.propagateSiteOptions(clusterSpec, sitexml);
    return sitexml;
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


  @Override //Client
  public void preflightValidateClusterConfiguration(ClusterDescription clusterSpec,
                                                    FileSystem clusterFS,
                                                    Path generatedConfDirPath,
                                                    boolean secure) throws
                                                                    HoyaException,
                                                                    IOException {
  }

  @Override
  public void validateClusterSpec(
    ClusterDescription clusterSpec) throws
                                    HoyaException {
    Map<String, String> am = clusterSpec.getRole(ROLE_HOYA_AM);
    if (am == null) {
      throw new BadCommandArgumentsException("No Hoya Application master declared" 
                                             + " in cluster specification");
    }
    providerUtils.validateNodeCount(ROLE_HOYA_AM,
                                    clusterSpec.getDesiredInstanceCount(
                                      ROLE_HOYA_AM,
                                      0), 1, 1);


  }

  @Override
  public Map<String, LocalResource> prepareAMAndConfigForLaunch(FileSystem clusterFS,
                                                                Configuration serviceConf,
                                                                ClusterDescription clusterSpec,
                                                                Path originConfDirPath,
                                                                Path generatedConfDirPath,
                                                                Configuration clientConfExtras)
    throws IOException, BadConfigException {
    
    Map<String, LocalResource> confResources =
      new HashMap<String, LocalResource>();
    return confResources;
  }

  /**
   * Update the AM resource with any local needs
   * @param capability capability to update
   */
  @Override
  public void prepareAMResourceRequirements(ClusterDescription clusterSpec,
                                            Resource capability) {
    capability.setMemory(clusterSpec.getRoleOptInt(
      HoyaKeys.ROLE_HOYA_AM,
      RoleKeys.YARN_MEMORY,
      capability.getMemory()));
    capability.setVirtualCores(clusterSpec.getRoleOptInt(
      HoyaKeys.ROLE_HOYA_AM, RoleKeys.YARN_CORES, capability.getVirtualCores()));
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

}
