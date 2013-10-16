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

package org.apache.hadoop.hoya.providers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.exceptions.BadConfigException;
import org.apache.hadoop.hoya.exceptions.HoyaException;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * This is the client-side provider, the bit
 * that helps create the template cluster spec,  
 * preflight checks the specification,
 * and 
 */
public interface ClientProvider extends ProviderCore {

  /**
   * Create the default cluster role instance for a named
   * cluster role; 
   *
   * @param rolename role name
   * @return a node that can be added to the JSON
   */
  Map<String, String> createDefaultClusterRole(String rolename) throws HoyaException;

  /**
   * This builds up the site configuration for the AM and downstream services;
   * the path is added to the cluster spec so that launchers in the 
   * AM can pick it up themselves. 
   *
   *
   * @param clusterFS filesystem
   * @param serviceConf conf used by the service
   * @param clusterSpec cluster specification
   * @param originConfDirPath the original config dir -treat as read only
   * @param generatedConfDirPath path to place generated artifacts
   * @param clientConfExtras optional extra configs to patch in last
   * @return a map of name to local resource to add to the AM launcher
   */
  Map<String, LocalResource> prepareAMAndConfigForLaunch(FileSystem clusterFS,
                                                         Configuration serviceConf,
                                                         ClusterDescription clusterSpec,
                                                         Path originConfDirPath,
                                                         Path generatedConfDirPath,
                                                         Configuration clientConfExtras) throws
                                                                                    IOException,
                                                                                    BadConfigException;

  /**
   * Get a map of all the default options for the cluster; values
   * that can be overridden by user defaults after
   * @return a possibly empyy map of default cluster options.
   */
  Map<String, String> getDefaultClusterOptions();

  /**
   * Build the conf dir from the service arguments
   * This the configuration used by HBase directly
   * @param clusterSpec this is the cluster specification used to define this
   * @return a map of the dynamic bindings for this Hoya instance
   */
  Map<String, String> buildSiteConfFromSpec(ClusterDescription clusterSpec)
    throws BadConfigException;

  /**
   * Update the AM resource with any local needs
   * @param capability capability to update
   */
  void prepareAMResourceRequirements(ClusterDescription clusterSpec,
                                     Resource capability);

  /**
   * Any operations to the service data before launching the AM
   * @param clusterSpec cspec
   * @param serviceData map of service data
   */
  void prepareAMServiceData(ClusterDescription clusterSpec,
                            Map<String, ByteBuffer> serviceData);

  
  /**
   * Build time review and update of the cluster specification
   * @param clusterSpec spec
   */
  void reviewAndUpdateClusterSpec(ClusterDescription clusterSpec) throws
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
   * @param clusterSpec cluster specification
   * @param clusterFS filesystem
   * @param generatedConfDirPath path to place generated artifacts
   * @param secure
   * @throws HoyaException
   */
  void preflightValidateClusterConfiguration(ClusterDescription clusterSpec,
                                             FileSystem clusterFS,
                                             Path generatedConfDirPath,
                                             boolean secure) throws
                                                             HoyaException,
                                                             IOException;
}
