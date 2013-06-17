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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hoya.HBaseCommands
import org.apache.hadoop.hoya.HoyaKeys
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.tools.YarnUtils
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.Token
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ContainerManagementProtocol
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier
import org.apache.hadoop.yarn.util.ProtoUtils
import org.apache.hadoop.yarn.util.Records

/**
 * Thread that runs on the AM to launch a region server.
 * In the Java examples these are usually non-static nested classes
 * of the AM. Groovy doesn't like non-static nested classes
 * -so this is isolated.
 * 
 * This could be brought back in to the AM by having a single method in the owner
 * to do everything, and have the runnable call it. For now: isolating things
 */
@Commons
@CompileStatic
class HoyaRegionServiceLauncher implements Runnable {
  final HoyaAppMaster owner
  
  // Allocated container
  final Container container;
  // Handle to communicate with ContainerManager
  ContainerManagementProtocol containerManager;

  HoyaRegionServiceLauncher(HoyaAppMaster owner, Container container) {
    this.owner = owner
    this.container = container
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
    PrivilegedConnectToCM action = new PrivilegedConnectToCM(owner,
                                                             cmAddress)
    ContainerManagementProtocol c = (ContainerManagementProtocol) user.doAs(action)
    containerManager = c;
    log.debug("Setting up container launch container for containerid=$container.id");

    ContainerLaunchContext ctx = Records
        .newRecord(ContainerLaunchContext.class);
/*

    String jobUserName = Env.mandatory(ApplicationConstants.Environment.USER
                                                           .key());
//    ctx.setUser(jobUserName);
    log.info("Setting user in ContainerLaunchContext to: $jobUserName");

*/
    //now build up the configuration data    
    
    // Set the environment
    Map<String, String> env = [:]
//    env[EnvMappings.ENV_HBASE_OPTS] = ConfigHelper.build_JVM_opts(env);
    env["HBASE_LOG_DIR"] = owner.buildHBaseContainerLogdir();

    ctx.setEnvironment(env);
    //local resources
    Map<String, LocalResource> localResources = [:];


    //add the configuration resources
    Path generatedConfPath = new Path(owner.getDFSConfDir())
    Map<String, LocalResource> confResources;
    confResources = YarnUtils.submitDirectory(owner.getClusterFS(),
                                              generatedConfPath,
                                              HoyaKeys.PROPAGATED_CONF_DIR_NAME)
    localResources.putAll(confResources)
    ctx.setLocalResources(localResources)
    
    //TODO: add binaries


    def command = []
    command << owner.buildHBaseBinPath().absolutePath

    //config dir is relative to the generated file
    command << HBaseCommands.ARG_CONFIG


    //TODO: get propagation to work
    command << HoyaKeys.PROPAGATED_CONF_DIR_NAME;
//    command << owner.getLocalConfDir()
    
    
    //role is region server
    command << HBaseCommands.REGION_SERVER
    command << HBaseCommands.ACTION_START
    
    //log details
    command << "1>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/out.txt";
    command << "2>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/err.txt";

    String cmdStr = command.join(" ")

    ctx.commands = [cmdStr]
    StartContainerRequest startReq = StartContainerRequest.newInstance(ctx,
                                                 container.getContainerToken())
    log.info("Starting container with command: $cmdStr");
    env.each { String k, String v ->
      log.info("$k=$v")
    }

    try {
      containerManager.startContainer(startReq);
    } catch (YarnException e) {
      log.error("Start container failed for :" +
                " containerId=${container.getId()} : $e", e);
      // TODO do we need to release this container?
      YarnUtils.stopContainer(containerManager, container.id)
      return
    } catch (IOException e) {
      log.error("Start container failed for :" +
                " containerId=${container.getId()} : $e", e);
      YarnUtils.stopContainer(containerManager, container.id)
      return
    }

    //here all is well. Build up a description
    ClusterDescription.ClusterNode node = new ClusterDescription.ClusterNode()
    List<String> nodeEnv=[]
    localResources.each {String key, LocalResource val ->
      nodeEnv << "$key=${YarnUtils.stringify(val.resource)}".toString()
    }
    node.command = cmdStr
    node.name = container.id
    node.environment = nodeEnv.toArray(new String[nodeEnv.size()])

    GetContainerStatusRequest statusReq =
      Records.newRecord(GetContainerStatusRequest.class);
    statusReq.containerId = container.id;
    GetContainerStatusResponse statusResp;
    try {
      statusResp = containerManager.getContainerStatus(statusReq);
      ContainerStatus containerStatus = statusResp.getStatus()
      node.diagnostics = containerStatus.diagnostics
      node.exitCode = containerStatus.exitStatus
    } catch (Exception e) {
      log.error("Failed to get status $e", e);
    }
    owner.addLaunchedContainerToCD(container.id ,node)
    

  }



}
