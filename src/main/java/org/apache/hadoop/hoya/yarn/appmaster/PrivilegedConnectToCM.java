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

package org.apache.hadoop.hoya.yarn.appmaster;


import org.apache.hadoop.yarn.api.ContainerManagementProtocol;

import java.net.InetSocketAddress;
import java.security.PrivilegedAction;

/**
 * Implement privileged connection to the CM
 * 
 * This is done in Java because of runtime errors
 * when trying to run <code>doAs()</code> operations
 * in groovy.
 */
public class PrivilegedConnectToCM implements PrivilegedAction<ContainerManagementProtocol> {
  final HoyaAppMaster appMaster;
  final InetSocketAddress cmAddress;

  public PrivilegedConnectToCM(HoyaAppMaster appMaster,
                               InetSocketAddress cmAddress) {
    this.appMaster = appMaster;
    this.cmAddress = cmAddress;
  }

  
//  @Override
  public ContainerManagementProtocol run() {
    return ((ContainerManagementProtocol) appMaster.getProxy(
          ContainerManagementProtocol.class,
          cmAddress));
  }
}