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
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.exceptions.HoyaException;

import java.util.List;

import static org.apache.hoya.api.RoleKeys.DEF_YARN_CORES;
import static org.apache.hoya.api.RoleKeys.DEF_YARN_MEMORY;
import static org.apache.hoya.api.RoleKeys.YARN_CORES;
import static org.apache.hoya.api.RoleKeys.YARN_MEMORY;

/**
 * An optional base class for providers
 */
public abstract class AbstractProviderCore extends Configured
      implements ProviderCore {

  protected AbstractProviderCore(Configuration conf) {
    super(conf);
  }

  protected AbstractProviderCore() {
  }

  
  
  /**
   * Validation common to all roles
   * @param clusterSpec
   * @throws HoyaException
   */
  @Override
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
}
