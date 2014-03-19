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
import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.core.conf.ConfTreeOperations;
import org.apache.hoya.core.conf.MapOperations;
import org.apache.hoya.core.launch.AbstractLauncher;
import org.apache.hoya.exceptions.BadClusterStateException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.tools.HoyaFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hoya.api.RoleKeys.DEF_YARN_CORES;
import static org.apache.hoya.api.RoleKeys.DEF_YARN_MEMORY;
import static org.apache.hoya.api.RoleKeys.ROLE_INSTANCES;
import static org.apache.hoya.api.RoleKeys.YARN_CORES;
import static org.apache.hoya.api.RoleKeys.YARN_MEMORY;

public abstract class AbstractClientProvider extends Configured {
  protected static final Logger log =
    LoggerFactory.getLogger(AbstractClientProvider.class);
  protected static final ProviderUtils providerUtils =
    new ProviderUtils(log);

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
   * Validate the instance definition.
   * @param clusterSpec
   */
  public void validateInstanceDefinition(AggregateConf instanceDefinition) throws
                                                                           HoyaException {

    List<ProviderRole> roles = getRoles();
    ConfTreeOperations resources =
      instanceDefinition.getResourceOperations();
    for (ProviderRole role : roles) {
      String name = role.name;
      MapOperations component = resources.getComponent(name);
      if (component != null) {
        String instances = component.get(ROLE_INSTANCES);
        if (instances == null) {
          String message = "No instance count provide for " + name;
          log.error("{} with \n{}", message,resources.toString());
          throw new BadClusterStateException(
            message);
        }
        String ram = component.get(YARN_MEMORY);
        String cores = component.get(YARN_CORES);


        providerUtils.getRoleResourceRequirement(ram,
                                                 DEF_YARN_MEMORY,
                                                 Integer.MAX_VALUE);
        providerUtils.getRoleResourceRequirement(cores,
                                                 DEF_YARN_CORES,
                                                 Integer.MAX_VALUE);
      }
    }
  }
  
  
  /**
   * Create the default cluster role instance for a named
   * cluster role; 
   *
   * @param rolename role name
   * @return a node that can be added to the JSON
   */
  @Deprecated
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

  public abstract Configuration getDefaultClusterConfiguration() throws
                                                          FileNotFoundException;


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
   * @param instanceDefinition cluster specification
   * @param clusterDirPath directory of the cluster
   * @param generatedConfDirPath path to place generated artifacts
   * @param secure flag to indicate that the cluster is secure
   * @throws HoyaException on any validation issue
   * @throws IOException on any IO problem
   */
  public void preflightValidateClusterConfiguration(HoyaFileSystem hoyaFileSystem,
                                                      String clustername,
                                                      Configuration configuration,
                                                      AggregateConf instanceDefinition,
                                                      Path clusterDirPath,
                                                      Path generatedConfDirPath,
                                                      boolean secure) throws
                                                                      HoyaException,
                                                                      IOException {
    validateInstanceDefinition(instanceDefinition);
  }

}
