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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.providers.ProviderRole;
import org.apache.hadoop.hoya.providers.ProviderService;
import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleInstance;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Thread that runs on the AM to launch a region server.
 */
public class RoleLauncher implements Runnable {
  protected static final Logger log =
    LoggerFactory.getLogger(RoleLauncher.class);

  private final HoyaAppMaster owner;

  // Allocated container
  private final Container container;
  private final String containerRole;
  private final Map<String, String> roleOptions;
  private final ProviderService provider;
  private final ClusterDescription clusterSpec;
  private final ProviderRole role;

  public RoleLauncher(HoyaAppMaster owner,
                      Container container,
                      ProviderRole role,
                      ProviderService provider,
                      ClusterDescription clusterSpec,
                      Map<String, String> roleOptions) {
    assert owner != null;
    assert container != null;
    assert role != null;
    assert roleOptions != null;
    assert provider != null;
    this.owner = owner;
    this.container = container;
    this.containerRole = role.name;
    this.role = role;
    this.roleOptions = roleOptions;
    this.provider = provider;
    this.clusterSpec = clusterSpec;
  }

  @Override
  public void run() {

    try {
      UserGroupInformation user =
        UserGroupInformation.createRemoteUser(container.getId().toString());
      String cmIpPortStr =
        container.getNodeId().getHost() + ":" + container.getNodeId().getPort();
      final InetSocketAddress cmAddress =
        NetUtils.createSocketAddr(cmIpPortStr);

      Token<ContainerTokenIdentifier> token =
        ConverterUtils.convertFromYarn(container.getContainerToken(),
                                       cmAddress);
      user.addToken(token);

      log.debug("Launching container {} into role {}",
                container.getId(),
                containerRole);
      FileSystem fs = owner.getClusterFS();
      Path generatedConfPath = new Path(owner.getDFSConfDir());

      ContainerLaunchContext ctx = Records
        .newRecord(ContainerLaunchContext.class);
      //now build up the configuration data    
      provider.buildContainerLaunchContext(ctx, fs,
                                           generatedConfPath,
                                           containerRole,
                                           clusterSpec,
                                           roleOptions);


 
      String commandLine = ctx.getCommands().get(0);
      RoleInstance instance = new RoleInstance(container);
      instance.buildUUID();
      log.info("Starting container with command: {}", commandLine);
      Map<String, LocalResource> lr = ctx.getLocalResources();
      List<String> nodeEnv = new ArrayList<String>();
      if (log.isDebugEnabled()) {
        log.debug("{} resources: ", lr.size());
        for (Map.Entry<String, LocalResource> entry : lr.entrySet()) {

          String key = entry.getKey();
          LocalResource val = entry.getValue();
          String envElt = key + "=" + HoyaUtils.stringify(val.getResource());
          log.debug(envElt);
        }
      }
      //log the env
      Map<String, String> environment = ctx.getEnvironment();
      log.debug("{} env variables: ", environment.size());

      for (Map.Entry<String, String> env : environment.entrySet()) {
        String envElt = String.format("%s=\"%s\"",
                                      env.getKey(),
                                      env.getValue());
        log.debug(envElt);
        nodeEnv.add(envElt);
      }
      instance.command = commandLine;
      instance.role = containerRole;
      instance.roleId = role.id;
      instance.environment = nodeEnv.toArray(new String[nodeEnv.size()]);
      owner.startContainer(container, ctx, instance);
    } catch (Exception e) {
      log.error(
        "Exception thrown while trying to start " + containerRole + ": " + e,
        e);
    } finally {
      owner.launchedThreadCompleted(this);
    }
  }

}
