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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.api.ClusterNode;
import org.apache.hadoop.hoya.exceptions.HoyaException;
import org.apache.hadoop.hoya.yarn.service.EventCallback;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.service.launcher.ExitCodeProvider;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ProviderService extends ProviderCore, Service,
                                         ExitCodeProvider {

  /**
   * Set up the entire container launch context
   * @param ctx
   * @param fs
   * @param generatedConfPath
   * @param role
   */
  void buildContainerLaunchContext(ContainerLaunchContext ctx,
                                   FileSystem fs,
                                   Path generatedConfPath, String role,
                                   ClusterDescription clusterSpec,
                                   Map<String, String> roleOptions) throws
                                                                    IOException,
                                                                    HoyaException;

  int getDefaultMasterInfoPort();

  String getSiteXMLFilename();

  List<String> buildProcessCommand(ClusterDescription cd,
                                   File confDir,
                                   Map<String, String> env,
                                   String masterCommand) throws
                                                         IOException,
                                                         HoyaException;

  void exec(ClusterDescription cd,
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
   * This is a validation of the application configuration on the AM.
   * Here is where things like the existence of keytabs and other
   * not-seen-client-side properties can be tested, before
   * the actual process is spawned. 
   * @param clusterSpec clusterSpecification
   * @param confDir configuration directory
   * @param secure flag to indicate that secure mode checks must exist
   * @throws IOException IO problemsn
   * @throws HoyaException any failure
   */
  void validateApplicationConfiguration(ClusterDescription clusterSpec,
                                        File confDir,
                                        boolean secure
                                       ) throws IOException, HoyaException;
}
