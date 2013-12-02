/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hoya.servicemonitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hoya.HoyaExitCodes;
import org.apache.hadoop.hoya.exceptions.ErrorStrings;
import org.apache.hadoop.hoya.exceptions.HoyaException;
import org.apache.hadoop.hoya.yarn.client.HoyaYarnClientImpl;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Probe for YARN application
 */
public class YarnApplicationProbe extends Probe {
  protected static final Logger log = LoggerFactory.getLogger(
    YarnApplicationProbe.class);

  /**
   * Yarn client service
   */
  private HoyaYarnClientImpl yarnClient;
  private final String clustername;
  private final String username;

  public YarnApplicationProbe(String clustername,
                              HoyaYarnClientImpl yarnClient,
                              String name,
                              Configuration conf, String username)
      throws IOException {
    super("Port probe " + name + " " + clustername,
          conf);
    this.clustername = clustername;
    this.yarnClient = yarnClient;
    this.username = username;
  }


  @Override
  public void init() throws IOException {
   
    log.info("Checking " + clustername );
  }

  /**
   * Try to connect to the (host,port); a failure to connect within
   * the specified timeout is a failure
   * @param livePing is the ping live: true for live; false for boot time
   * @return the outcome
   */
  @Override
  public ProbeStatus ping(boolean livePing) {
    
    ProbeStatus status = new ProbeStatus();
    try {

      List<ApplicationReport> instances =
        yarnClient.listHoyaInstances(username);
      ApplicationReport instance =
        yarnClient.findClusterInInstanceList(instances, clustername);
      if (null == instance) {
        throw new HoyaException(HoyaExitCodes.EXIT_UNKNOWN_HOYA_CLUSTER,
                                ErrorStrings.E_UNKNOWN_CLUSTER
                                + ": \"" + clustername + "\"");

      }

      status.succeed(this);
    } catch (Exception e) {
      status.fail(this, e);
    }
    return status;

  }
}
