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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hoya.tools.CoreFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppMasterLauncher extends AbstractLauncher {


  private static final Logger log =
    LoggerFactory.getLogger(AppMasterLauncher.class);

  protected final YarnClientApplication application;
  private final String name;
  private final String type;
  private final ApplicationSubmissionContext submissionContext;
  private final ApplicationId appId;
  private int maxAppAttempts = 2;
  private boolean keepContainersOverRestarts = true;
  private String queueName = YarnConfiguration.DEFAULT_QUEUE_NAME;
  private int queuePriority = 1;
  private final Resource resource = Records.newRecord(Resource.class);

  public AppMasterLauncher(String name,
                           String type,
                           Configuration conf,
                           CoreFileSystem fs,
                           YarnClientApplication application) {
    super(conf, fs);
    this.application = application;
    this.name = name;
    this.type = type;

    submissionContext = application.getApplicationSubmissionContext();
    appId = submissionContext.getApplicationId();
    // set the application name;
    submissionContext.setApplicationName(name);
    // app type used in service enum;
    submissionContext.setApplicationType(type);

  }

  public void setMaxAppAttempts(int maxAppAttempts) {
    this.maxAppAttempts = maxAppAttempts;
  }

  public void setKeepContainersOverRestarts(boolean keepContainersOverRestarts) {
    this.keepContainersOverRestarts = keepContainersOverRestarts;
  }


  public Resource getResource() {
    return resource;
  }

  public void setMemory(int memory) {
    resource.setMemory(memory);
  }

  public void setCores(int cores) {
    resource.setVirtualCores(cores);
  }

  /**
   * Complete the launch context (copy in env vars, etc).
   * @return the container to launch
   */
  public ApplicationSubmissionContext completeAppMasterLaunch() {
    completeContainerLaunch();

    //queue priority
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(queuePriority);
    submissionContext.setPriority(pri);

    // Set the queue to which this application is to be submitted in the RM
    // Queue for App master

    submissionContext.setQueue(queueName);
    
    //container requirements
    submissionContext.setResource(getResource());

    if (keepContainersOverRestarts &&
        AMRestartSupport.keepContainersAcrossSubmissions(submissionContext)) {
      log.debug("Requesting cluster stays running over AM failure");
    }

    submissionContext.setAMContainerSpec(containerLaunchContext);

    return submissionContext;

  }
}
