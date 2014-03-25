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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.service.launcher.ExitCodeProvider;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.core.conf.MapOperations;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.yarn.appmaster.state.StateAccessForProviders;
import org.apache.hoya.yarn.appmaster.web.rest.agent.AgentRestOperations;
import org.apache.hoya.yarn.service.EventCallback;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

public interface ProviderService extends ProviderCore, Service,
                                         ExitCodeProvider {

  /**
   * Set up the entire container launch context
   * @param ctx
   * @param instanceDefinition
   * @param container
   * @param role
   * @param hoyaFileSystem
   * @param generatedConfPath
   * @param appComponent
   * @param containerTmpDirPath
   */
  void buildContainerLaunchContext(ContainerLaunchContext ctx,
                                   AggregateConf instanceDefinition,
                                   Container container,
                                   String role,
                                   HoyaFileSystem hoyaFileSystem,
                                   Path generatedConfPath,
                                   MapOperations resourceComponent,
                                   MapOperations appComponent,
                                   Path containerTmpDirPath) throws
                                                                    IOException,
                                                                    HoyaException;

  /**
   * Execute a process in the AM
   * @param instanceDefinition cluster description
   * @param confDir configuration directory
   * @param env environment
   * @param execInProgress the callback for the exec events
   * @return true if a process was actually started
   * @throws IOException
   * @throws HoyaException
   */
  boolean exec(AggregateConf instanceDefinition,
               File confDir,
               Map<String, String> env,
               EventCallback execInProgress) throws IOException,
                                                 HoyaException;

  /**
   * Scan through the roles and see if it is supported.
   * @param role role to look for
   * @return true if the role is known about -and therefore
   * that a launcher thread can be deployed to launch it
   */
  boolean isSupportedRole(String role);

  /**
   * Load a specific XML configuration file for the provider config
   * @param confDir configuration directory
   * @param siteXMLFilename provider-specific filename
   * @return a configuration to be included in status
   * @throws BadCommandArgumentsException
   * @throws IOException
   */
  Configuration loadProviderConfigurationInformation(File confDir)
    throws BadCommandArgumentsException, IOException;

  /**
   * This is a validation of the application configuration on the AM.
   * Here is where things like the existence of keytabs and other
   * not-seen-client-side properties can be tested, before
   * the actual process is spawned. 
   * @param instanceDefinition clusterSpecification
   * @param confDir configuration directory
   * @param secure flag to indicate that secure mode checks must exist
   * @throws IOException IO problemsn
   * @throws HoyaException any failure
   */
  void validateApplicationConfiguration(AggregateConf instanceDefinition,
                                        File confDir,
                                        boolean secure
                                       ) throws IOException, HoyaException;

  /*
     * Build the provider status, can be empty
     * @return the provider status - map of entries to add to the info section
     */
  Map<String, String> buildProviderStatus();
  
  /**
   * Build a map of data intended for the AM webapp that is specific
   * about this provider. The key is some text to be displayed, and the
   * value can be a URL that will create an anchor over the key text.
   * 
   * If no anchor is needed/desired, insert the key with a null value.
   * @return
   */
  Map<String,URL> buildMonitorDetails(ClusterDescription clusterSpec);

  /**
   * bind operation -invoked before the service is started
   * @param stateAccessor interface offering read access to the state
   */
  void bind(StateAccessForProviders stateAccessor);

  /**
   * Returns the agent rest operations interface.
   * @return  the interface if available, null otherwise.
   */
  AgentRestOperations getAgentRestOperations();
}
