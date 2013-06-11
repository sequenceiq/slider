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

import groovy.transform.CompileStatic
import groovy.util.logging.Commons
import org.apache.hadoop.hoya.tools.Env
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ContainerManager
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hadoop.yarn.util.Records
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.ProtoUtils;
import java.security.PrivilegedAction;


/**
 * Thread that runs on the AM to launch a region server.
 * In the Java examples these are usually non-static nested classes
 * of the AM. Groovy doesn't like non-static nested classes
 * -so this is isolated.
 * The class just needs to be set up with its binding info
 */
@Commons
@CompileStatic
class HoyaRegionServiceLauncher implements Runnable {
  final HoyaAppMaster owner
  
  // Allocated container
  final Container container;
  // Handle to communicate with ContainerManager
  ContainerManager containerManager;

  HoyaRegionServiceLauncher(HoyaAppMaster owner, Container container) {
    this.owner = owner
    this.container = container
  }

  /**
   * Implement privileged connection to the CM
   */
  private class PrivilegedConnectToCM implements PrivilegedAction<ContainerManager> {
    final InetSocketAddress cmAddress;

    PrivilegedConnectToCM(InetSocketAddress cmAddress) {
      this.cmAddress = cmAddress
    }

    @Override
    ContainerManager run() {
      return connectToCM(cmAddress)
    }
  }
  
  @Override
  void run() {
    UserGroupInformation user =
        UserGroupInformation.createRemoteUser(container.getId().toString());
    String cmIpPortStr = "$container.nodeId.host:$container.nodeId.port";
    final InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);

    Token<ContainerTokenIdentifier> token =
        ProtoUtils.convertFromProtoFormat(container.getContainerToken(), cmAddress);
    user.addToken(token);

      // Connect to ContainerManager
    ContainerManager c = user.doAs(new PrivilegedConnectToCM(cmAddress))
    
    //connectToCM(cmAddress);
    log.debug("Setting up container launch container for containerid=$container.id");

    ContainerLaunchContext ctx = Records
        .newRecord(ContainerLaunchContext.class);

    String jobUserName = Env.mandatory(ApplicationConstants.Environment.USER
                                                           .key());
//    ctx.setUser(jobUserName);
    log.info("Setting user in ContainerLaunchContext to: $jobUserName");

    // Set the environment
    Map<String, String> env = owner.buildEnvMap();
    ctx.setEnvironment(env);
    //local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    def command = []
    command << ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java"
    command << ServiceLauncher.ENTRY_POINT
    command << HoyaRegionServiceArgs.CLASSNAME
    command << HoyaRegionServiceArgs.ARG_DEBUG
    command << HoyaMasterServiceArgs.ARG_NAME << "name"
    command << HoyaMasterServiceArgs.ARG_ACTION << "create"
    //path can be unqualified
    command << HoyaMasterServiceArgs.ARG_PATH << "services/hoya/"
    command << "1>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/out.txt";
    command << "2>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/err.txt";

    String cmdStr = command.join(" ")
    log.info("Completed setting up region service command $cmdStr");

    ctx.commands = [cmdStr]
    StartContainerRequest startReq = StartContainerRequest.newInstance(ctx, container.getContainerToken())
    //StartContainerRequest startReq = Records
    //    .newRecord(StartContainerRequest.class);
    //startReq.containerLaunchContext = ctx;
  //  startReq.
//    startReq.container = container;
    try {
      containerManager.startContainer(startReq);
    } catch (YarnException e) {
      log.error("Start container failed for :" +
                " containerId=${container.getId()} : $e", e);
      // TODO do we need to release this container?
    } catch (IOException e) {
      log.error("Start container failed for :" +
                " containerId=${container.getId()} : $e", e);
    }

    GetContainerStatusRequest statusReq =
      Records.newRecord(GetContainerStatusRequest.class);
    statusReq.containerId = container.id;
    GetContainerStatusResponse statusResp;
    try {
      statusResp = containerManager.getContainerStatus(statusReq);
      log.info("Container Status, id=${container.id}," +
               " status=${statusResp.status}");
    } catch (Exception e) {
      log.error("Failed to get status $e", e);
    }
  }

  private ContainerManager connectToCM(final InetSocketAddress cmAddress) {
    
    log.debug("Connecting to ContainerManager for containerid=$container.id");
    log.info("Connecting to ContainerManager at " + cmAddress);
    this.containerManager = ((ContainerManager) owner.getProxy(ContainerManager.class,
                                               cmAddress));
    containerManager
  }

}
