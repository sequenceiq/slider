/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hoya.yarn.appmaster

import com.beust.jcommander.Parameter
import groovy.transform.CompileStatic
import org.apache.hadoop.hoya.yarn.CommonArgs

/**
 * Any arguments common to all the services but not the client
 */
@CompileStatic
class HoyaServiceCommonArgs extends CommonArgs {

  /**
   * Path for the ZK instance (required)
   */
  public static final String ARG_ZK_PATH = "--zkpath"
  public static final String ARG_RM_ADDR = "--rm"
  public static final String ARG_FILESYSTEM = "--filesystem"


  @Parameter(names = "--zkpath",
      description = "Zookeeper cluster path",
      required = true)
  public String zkPath;

  @Parameter(names = "--rm",
      description = "Resource manager hostname:port ",
      required = true)
  public String rmAddress;

  @Parameter(names = "--filesystem",
      description = "HBase filesystem name",
      required = true)
  public String filesystem;


  public HoyaServiceCommonArgs(String[] args) {
    super(args)
  }
}
