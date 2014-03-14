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

package org.apache.hoya.core.launch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hoya.core.conf.MapOperations;
import org.apache.hoya.tools.CoreFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Launcher of applications: base class
 */
public abstract class AbstractLauncher extends Configured {
  private static final Logger log =
    LoggerFactory.getLogger(AbstractLauncher.class);
  /**
   * Filesystem to use for the launch
   */
  protected final CoreFileSystem fs;
  /**
   * Env vars; set up at final launch stage
   */
  protected final Map<String, String> envVars = new HashMap<String, String>();
  protected final MapOperations env = new MapOperations(envVars);
  protected final ContainerLaunchContext containerLaunchContext =
    Records.newRecord(ContainerLaunchContext.class);
  protected final List<String> commands = new ArrayList<String>(20);
  protected final Map<String, LocalResource> localResources =
    new HashMap<String, LocalResource>();
  private final Map<String, ByteBuffer> serviceData =
    new HashMap<String, ByteBuffer>();
  private ByteBuffer tokens = null;

  protected AbstractLauncher(Configuration conf,
                             CoreFileSystem fs) {
    super(conf);
    this.fs = fs;
  }

  public AbstractLauncher(CoreFileSystem fs) {
    this.fs = fs;
  }

  /**
   * Get the container. Until "completed", this isn't valid to launch.
   * @return the container to launch
   */
  public ContainerLaunchContext getContainerLaunchContext() {
    return containerLaunchContext;
  }

  /**
   * Get the env vars to work on
   * @return env vars
   */
  public MapOperations getEnv() {
    return env;
  }

  public List<String> getCommands() {
    return commands;
  }

  public Map<String, LocalResource> getLocalResources() {
    return localResources;
  }

  public Map<String, ByteBuffer> getServiceData() {
    return serviceData;
  }

  public void setEnv(String var, String value) {
    env.put(var, value);
  }

  public void addCommand(String cmd) {
    commands.add(cmd);
  }


  /**
   * Append the output and error files to the tail of the command
   * @param stdout out
   * @param stderr error
   */
  public void addOutAndErrFiles(String stdout, String stderr) {
    // write out the path output
    commands.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" +
                 stdout);
    commands.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" +
                 stderr);
  }

  public void setTokens(ByteBuffer tokens) {
    this.tokens = tokens;
  }

  public ByteBuffer getTokens() {
    return tokens;
  }

  /**
   * Complete the launch context (copy in env vars, etc).
   * @return the container to launch
   */
  public ContainerLaunchContext completeContainerLaunch() {
    dumpLocalResources();
    String cmdStr = HoyaUtils.join(commands, " ");
    log.debug("Completed setting up container command {}", cmdStr);
    //fix the env variables
    containerLaunchContext.setEnvironment(env);
    //service data
    containerLaunchContext.setServiceData(serviceData);
    containerLaunchContext.setTokens(tokens);


    return containerLaunchContext;
  }

  /**
   * Dump local resources at debug level
   */
  private void dumpLocalResources() {
    if (log.isDebugEnabled()) {
      log.debug("{} resources: ", localResources.size());
      for (Map.Entry<String, LocalResource> entry : localResources.entrySet()) {

        String key = entry.getKey();
        LocalResource val = entry.getValue();
        log.debug(key + "=" + HoyaUtils.stringify(val.getResource()));
      }
    }
  }

  /**
   * This is critical for an insecure cluster -it passes
   * down the username to YARN, and so gives the code running
   * in containers the rights it needs to work with
   * data.
   * @throws IOException problems working with current user
   */
  private void propagateUsernameInInsecureCluster() throws IOException {
    //insecure cluster: propagate user name via env variable
    String userName = UserGroupInformation.getCurrentUser().getUserName();
    env.put("HADOOP_USER_NAME", userName);
  }
}
