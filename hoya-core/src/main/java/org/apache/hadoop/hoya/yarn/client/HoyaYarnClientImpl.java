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

package org.apache.hadoop.hoya.yarn.client;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hoya.HoyaKeys;
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hadoop.hoya.exceptions.HoyaException;
import org.apache.hadoop.hoya.tools.Duration;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.hoya.yarn.params.ActionForceKillArgs;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A class that extends visibility to some of the YarnClientImpl
 * members and data structures, and factors out pure-YARN operations
 * from the hoya entry point service
 */
public class HoyaYarnClientImpl extends YarnClientImpl {
  protected static final Logger
    log = LoggerFactory.getLogger(HoyaYarnClientImpl.class);

  /**
   * Get the RM Client RPC interface
   * @return an RPC interface valid after initialization and authentication
   */
  public ApplicationClientProtocol getRmClient() {
    return rmClient;
  }


  /**
   * List Hoya instances belonging to a specific user
   * @param user user: "" means all users
   * @return a possibly empty list of Hoya AMs
   */
  public List<ApplicationReport> listHoyaInstances(String user)
    throws YarnException, IOException {
    Set<String> types = new HashSet<String>(1);
    types.add(HoyaKeys.APP_TYPE);
    List<ApplicationReport> allApps = getApplications(types);
    List<ApplicationReport> results = new ArrayList<ApplicationReport>();
    for (ApplicationReport report : allApps) {
      if (user == null || user.equals(report.getUser())) {
        results.add(report);
      }
    }
    return results;
  }


  /**
   * find all instances of a specific app -if there is >1 in the cluster,
   * this returns them all
   * @param user user
   * @param appname application name
   * @return the list of all matching application instances
   */
  @VisibleForTesting
  public List<ApplicationReport> findAllInstances(String user,
                                                  String appname) throws
                                                                  IOException,
                                                                  YarnException {
    List<ApplicationReport> instances = listHoyaInstances(user);
    List<ApplicationReport> results =
      new ArrayList<ApplicationReport>(instances.size());
    for (ApplicationReport report : instances) {
      if (report.getName().equals(appname)) {
        results.add(report);
      }
    }
    return results;
  }
  
  /**
   * Helper method to determine if a cluster application is running -or
   * is earlier in the lifecycle
   * @param app application report
   * @return true if the application is considered live
   */
  public boolean isApplicationLive(ApplicationReport app) {
    return app.getYarnApplicationState().ordinal() <=
           YarnApplicationState.RUNNING.ordinal();
  }


  /**
   * Kill a running application
   * @param applicationId
   * @return the response
   * @throws YarnException YARN problems
   * @throws IOException IO problems
   */
  public  KillApplicationResponse killRunningApplication(ApplicationId applicationId,
                                                         String reason) throws
                                                                        YarnException,
                                                                        IOException {
    log.info("Killing application {} - {}", applicationId.getClusterTimestamp(),
             reason);
    KillApplicationRequest request =
      Records.newRecord(KillApplicationRequest.class);
    request.setApplicationId(applicationId);
    return getRmClient().forceKillApplication(request);
  }

  private String getUsername() throws IOException {
    return UserGroupInformation.getCurrentUser().getShortUserName();
  }
  /**
   * Force kill a yarn application by ID. No niceities here
   */
  public void emergencyForceKill(String applicationId) throws
                                                            YarnException,
                                                            IOException {
    

    if (ActionForceKillArgs.ALL.equals(applicationId)) {
      // user wants all hoya applications killed
      String user = getUsername();
      log.info("Killing all applications belonging to {}", user);
      Collection<ApplicationReport> instances = listHoyaInstances(user);
      for (ApplicationReport instance : instances) {
        if (isApplicationLive(instance)) {
          ApplicationId appId = instance.getApplicationId();
          log.info("Killing Application {}", appId);

          killRunningApplication(appId, "forced kill");
        }
      }
    } else {
      ApplicationId appId = ConverterUtils.toApplicationId(applicationId);

      log.info("Killing Application {}", applicationId);

      killRunningApplication(appId, "forced kill");
    }
  }

  /**
   * Monitor the submitted application for reaching the requested state.
   * Will also report if the app reaches a later state (failed, killed, etc)
   * Kill application if duration!= null & time expires. 
   * @param appId Application Id of application to be monitored
   * @param duration how long to wait -must be more than 0
   * @param desiredState desired state.
   * @return the application report -null on a timeout
   * @throws YarnException
   * @throws IOException
   */
  public ApplicationReport monitorAppToState(
    ApplicationId appId, YarnApplicationState desiredState, Duration duration)
    throws YarnException, IOException {

    if (appId == null) {
      throw new BadCommandArgumentsException("null application ID");
    }
    if (duration.limit <= 0) {
      throw new BadCommandArgumentsException("Invalid monitoring duration");
    }
    log.debug("Waiting {} millis for app to reach state {} ",
              duration.limit,
              desiredState);
    duration.start();
    while (true) {

      // Get application report for the appId we are interested in

      ApplicationReport r = getApplicationReport(appId);

      log.debug("queried status is\n{}",
                new HoyaUtils.OnDemandReportStringifier(r));

      YarnApplicationState state = r.getYarnApplicationState();
      if (state.ordinal() >= desiredState.ordinal()) {
        log.debug("App in desired state (or higher) :{}", state);
        return r;
      }
      if (duration.getLimitExceeded()) {
        log.debug(
          "Wait limit of {} millis to get to state {}, exceeded; app status\n {}",
          duration.limit,
          desiredState,
          new HoyaUtils.OnDemandReportStringifier(r));
        return null;
      }

      // sleep 1s.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {
        log.debug("Thread sleep in monitoring loop interrupted");
      }
    }
  }

  /**
   * find all live instances of a specific app -if there is >1 in the cluster,
   * this returns them all. State should be running or less
   * @param user user
   * @param appname application name
   * @return the list of all matching application instances
   */
  public List<ApplicationReport> findAllLiveInstances(String user,
                                                      String appname) throws
                                                                      YarnException,
                                                                      IOException {
    List<ApplicationReport> instances = listHoyaInstances(user);
    List<ApplicationReport> results =
      new ArrayList<ApplicationReport>(instances.size());
    for (ApplicationReport app : instances) {
      if (app.getName().equals(appname)
          && isApplicationLive(app)) {
        results.add(app);
      }
    }
    return results;
  }

  public ApplicationReport findClusterInInstanceList(List<ApplicationReport> instances,
                                                     String appname) {
    ApplicationReport found = null;
    ApplicationReport foundAndLive = null;
    for (ApplicationReport app : instances) {
      if (app.getName().equals(appname)) {
        found = app;
        if (isApplicationLive(app)) {
          foundAndLive = app;
        }
      }
    }
    if (foundAndLive != null) {
      found = foundAndLive;
    }
    return found;
  }


}
