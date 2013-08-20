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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hoya.providers.ProviderCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class HBaseClusterCore extends Configured implements
                                                          ProviderCore,
                                                          HBaseCommands {

  protected static final Logger log =
    LoggerFactory.getLogger(HBaseClusterCore.class);
  protected static final String NAME = "hbase";

  protected HBaseClusterCore(Configuration conf) {
    super(conf);
  }
  
  protected static final List<String> ROLES = new ArrayList<String>(1);

  protected static final String ROLE_WORKER = "worker";
  protected static final String ROLE_MASTER = "master";

  static {
    ROLES.add(ROLE_WORKER);
    ROLES.add(ROLE_MASTER);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<String> getRoles() {
    return ROLES;
  }


}
