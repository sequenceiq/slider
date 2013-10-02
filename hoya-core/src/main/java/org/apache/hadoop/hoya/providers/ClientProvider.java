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
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException;
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

  Map<String, LocalResource> prepareAMAndConfigForLaunch(FileSystem clusterFS,
                                                         Configuration serviceConf,
                                                         ClusterDescription clusterSpec,
                                                         Path originConfDirPath,
                                                         Path generatedConfDirPath) throws
                                                                                    IOException,
                                                                                    BadConfigException;

  Map<String, String> getDefaultClusterOptions();

  Map<String, String> buildSiteConfFromSpec(ClusterDescription clusterSpec)
    throws BadConfigException;
  
  void prepareAMResourceRequirements(ClusterDescription clusterSpec,
                                     Resource capability);

  void prepareAMServiceData(ClusterDescription clusterSpec,
                            Map<String, ByteBuffer> serviceData);

  void reviewAndUpdateClusterSpec(ClusterDescription clusterSpec) throws
                                                                  HoyaException;

}
