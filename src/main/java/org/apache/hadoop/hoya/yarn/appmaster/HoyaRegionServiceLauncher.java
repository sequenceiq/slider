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
import org.apache.hadoop.hoya.providers.hbase.HBaseCommands;
import org.apache.hadoop.hoya.HoyaKeys;
import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.api.ClusterNode;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Thread that runs on the AM to launch a region server.
 */
public class HoyaRegionServiceLauncher implements Runnable {
  protected static final Logger log =
    LoggerFactory.getLogger(HoyaRegionServiceLauncher.class);

  private final HoyaAppMaster owner;

  // Allocated container
  private final Container container;
  private final String hbaseRole;
  private final String hoyaRole;

  public HoyaRegionServiceLauncher(HoyaAppMaster owner,
                                   Container container,
                                   String hbaseRole,
                                   String hoyaRole) {
    assert owner != null;
    assert container != null;
    assert hbaseRole != null;
    assert hoyaRole != null;
    this.owner = owner;
    this.container = container;
    this.hbaseRole = hbaseRole;
    this.hoyaRole = hoyaRole;
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

      log.debug("Setting up container launch container for containerid={}",
                container.getId());

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
      Map<String, String> env = new HashMap<String, String>();
//    env[EnvMappings.ENV_HBASE_OPTS] = ConfigHelper.build_JVM_opts(env);
      env.put(HoyaKeys.HBASE_LOG_DIR, owner.buildHBaseContainerLogdir());

      ctx.setEnvironment(env);
      //local resources
      Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

      //add the configuration resources
      Path generatedConfPath = new Path(owner.getDFSConfDir());
      Map<String, LocalResource> confResources;
      FileSystem fs = owner.getClusterFS();
      confResources = HoyaUtils.submitDirectory(fs,
                                                generatedConfPath,
                                                HoyaKeys.PROPAGATED_CONF_DIR_NAME);
      localResources.putAll(confResources);

      //Add binaries
      ClusterDescription clusterSpec = owner.clusterDescription;
      //now add the image if it was set
      if (clusterSpec.imagePath != null) {
        Path imagePath = new Path(clusterSpec.imagePath);
        log.info("using image path {}", imagePath);
        HoyaUtils.maybeAddImagePath(fs, localResources, imagePath);
      }
      ctx.setLocalResources(localResources);

      List<String> command = new ArrayList<String>();
      //this must stay relative if it is an image
      command.add(owner.buildHBaseBinPath(clusterSpec).toString());

      //config dir is relative to the generated file
      command.add(HBaseCommands.ARG_CONFIG);
      command.add(HoyaKeys.PROPAGATED_CONF_DIR_NAME);
      //role is region server
      command.add(hbaseRole);
      command.add(HBaseCommands.ACTION_START);

      //log details
      command.add(
        "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/out.txt");
      command.add(
        "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/err.txt");

      String cmdStr = HoyaUtils.join(command, " ");

      List<String> commands = new ArrayList<String>();
      commands.add(cmdStr);
      ctx.setCommands(commands);
      log.info("Starting container with command: {}", cmdStr);

      ClusterNode node = new ClusterNode();
      List<String> nodeEnv = new ArrayList<String>();
      for (Map.Entry<String, LocalResource> entry : localResources.entrySet()) {

        String key = entry.getKey();
        LocalResource val = entry.getValue();
        String envElt = key + "=" + HoyaUtils.stringify(val.getResource());
        nodeEnv.add(envElt);
        log.info(envElt);
      }
      node.command = cmdStr;
      node.name = container.getId().toString();
      node.role = hoyaRole;
      node.environment = nodeEnv.toArray(new String[nodeEnv.size()]);
      owner.startContainer(container, ctx, node);
    } catch (IOException e) {
      log.error("Exception thrown while trying to start region server: " + e,
                e);
    }
  }

}
