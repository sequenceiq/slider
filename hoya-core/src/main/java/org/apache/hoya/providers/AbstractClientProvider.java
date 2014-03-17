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

package org.apache.hoya.providers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.core.conf.ConfTreeOperations;
import org.apache.hoya.core.launch.AbstractLauncher;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.tools.HoyaFileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hoya.api.RoleKeys.*;

public abstract class AbstractClientProvider extends Configured {

  public static final String PROVIDER_RESOURCE_BASE =
    "org/apache/hoya/providers/";
  public static final String PROVIDER_RESOURCE_BASE_ROOT =
    "/" + PROVIDER_RESOURCE_BASE;

  public AbstractClientProvider(Configuration conf) {
    super(conf);
  }

  public AbstractClientProvider() {
  }

  public abstract String getName();

  public abstract List<ProviderRole> getRoles();

  /**
   * Validation common to all roles
   * @param clusterSpec
   * @throws HoyaException
   */
  @Deprecated
  public void validateClusterSpec(ClusterDescription clusterSpec) throws
                                                                  HoyaException {
    List<ProviderRole> roles = getRoles();
    for (ProviderRole role : roles) {
      String name = role.name;
      clusterSpec.getRoleResourceRequirement(name,
                                             YARN_MEMORY,
                                             DEF_YARN_MEMORY,
                                             Integer.MAX_VALUE);
      clusterSpec.getRoleResourceRequirement(name,
                                             YARN_CORES,
                                             DEF_YARN_CORES,
                                             Integer.MAX_VALUE);
    }
  }

  /**
   * Create the default cluster role instance for a named
   * cluster role; 
   *
   * @param rolename role name
   * @return a node that can be added to the JSON
   */
  public Map<String, String> createDefaultClusterRole(String rolename) throws
                                                                         HoyaException,
                                                                         IOException {
    return new HashMap<String, String>();
  }

  /**
   * Any provider-side alteration of a configuration can take place here.
   * @param aggregateConf config to patch
   * @throws IOException IO problems
   * @throws HoyaException Hoya-specific issues
   */
  public void prepareInstanceConfiguration(AggregateConf aggregateConf) throws
                                                                    HoyaException,
                                                                    IOException {
    //default: do nothing
  }
    
  
  
  /**
   * This builds up the site configuration for the AM and downstream services;
   * the path is added to the cluster spec so that launchers in the 
   * AM can pick it up themselves. 
   *
   * @param hoyaFileSystem filesystem
   * @param serviceConf conf used by the service
   * @param clusterSpec cluster specification
   * @param originConfDirPath the original config dir -treat as read only
   * @param generatedConfDirPath path to place generated artifacts
   * @param clientConfExtras optional extra configs to patch in last
   * @param libdir relative directory to place resources
   * @param tempPath path in the cluster FS for temp files
   * @return a map of name to local resource to add to the AM launcher
   * @throws IOException IO problems
   * @throws HoyaException Hoya-specific issues
   */
  @Deprecated
  public abstract Map<String, LocalResource> prepareAMAndConfigForLaunch(HoyaFileSystem hoyaFileSystem,
                                                                  Configuration serviceConf,
                                                                  ClusterDescription clusterSpec,
                                                                  Path originConfDirPath,
                                                                  Path generatedConfDirPath,
                                                                  Configuration clientConfExtras,
                                                                  String libdir,
                                                                  Path tempPath) throws
                                                                                 IOException,
                                                                                 HoyaException;

  /**
   * Get a map of all the default options for the cluster; values
   * that can be overridden by user defaults after
   * @return a possibly empyy map of default cluster options.
   */
  public abstract Configuration getDefaultClusterConfiguration() throws
                                                          FileNotFoundException;


  /**
   * Update the AM resource with any local needs
   * @param capability capability to update
   */
  public abstract void prepareAMResourceRequirements(ClusterDescription clusterSpec,
                                              Resource capability);

  /**
   * Any operations to the service data before launching the AM
   * @param clusterSpec cspec
   * @param serviceData map of service data
   */
  public abstract void prepareAMServiceData(ClusterDescription clusterSpec,
                                     Map<String, ByteBuffer> serviceData);


  /**
   * The Hoya AM sets up all the dependency JARs above hoya.jar itself
   * {@inheritDoc}
   */
  public void prepareAMAndConfigForLaunch(HoyaFileSystem hoyaFileSystem,
                                          Configuration serviceConf,
                                          AbstractLauncher launcher,
                                          AggregateConf instanceDescription,
                                          Path originConfDirPath,
                                          Path generatedConfDirPath,
                                          Configuration clientConfExtras,
                                          String libdir,
                                          Path tempPath)
    throws IOException, HoyaException {
    
  }
  
  /**
   * Load in and merge in templates. Null arguments means "no such template"
   * @param instanceConf instance to patch 
   * @param internalTemplate patch to internal.json
   * @param resourceTemplate path to resources.json
   * @param appConfTemplate path to app_conf.json
   * @throws IOException any IO problems
   */
  protected void mergeTemplates(AggregateConf instanceConf,
                                String internalTemplate,
                                String resourceTemplate,
                                String appConfTemplate) throws IOException {
    if (internalTemplate != null) {
      ConfTreeOperations template =
        ConfTreeOperations.fromResource(internalTemplate);
      instanceConf.getInternalOperations()
                  .mergeWithoutOverwrite(template.confTree);
    }

    if (resourceTemplate != null) {
      ConfTreeOperations resTemplate =
        ConfTreeOperations.fromResource(resourceTemplate);
      instanceConf.getResourceOperations()
                   .mergeWithoutOverwrite(resTemplate.confTree);
    }
   
    if (appConfTemplate != null) {
      ConfTreeOperations template =
        ConfTreeOperations.fromResource(appConfTemplate);
      instanceConf.getAppConfOperations()
                   .mergeWithoutOverwrite(template.confTree);
    }
    
  }

  /**
   * Build time review and update of the cluster specification
   * @param clusterSpec spec
   */
  public abstract void reviewAndUpdateClusterSpec(ClusterDescription clusterSpec) throws
                                                                           HoyaException;

  /**
   * This is called pre-launch to validate that the cluster specification
   * is valid. This can include checking that the security options
   * are in the site files prior to launch, that there are no conflicting operations
   * etc.
   *
   * This check is made prior to every launch of the cluster -so can 
   * pick up problems which manually edited cluster files have added,
   * or from specification files from previous versions.
   *
   * The provider MUST NOT change the remote specification. This is
   * purely a pre-launch validation of options.
   *
   *
   * @param hoyaFileSystem filesystem
   * @param clustername name of the cluster
   * @param configuration cluster configuration
   * @param clusterSpec cluster specification
   * @param clusterDirPath directory of the cluster
   * @param generatedConfDirPath path to place generated artifacts
   * @param secure flag to indicate that the cluster is secure
   * @throws HoyaException on any validation issue
   * @throws IOException on any IO problem
   */
  public abstract void preflightValidateClusterConfiguration(HoyaFileSystem hoyaFileSystem,
                                                      String clustername,
                                                      Configuration configuration,
                                                      ClusterDescription clusterSpec,
                                                      Path clusterDirPath,
                                                      Path generatedConfDirPath,
                                                      boolean secure) throws
                                                                      HoyaException,
                                                                      IOException;

}
