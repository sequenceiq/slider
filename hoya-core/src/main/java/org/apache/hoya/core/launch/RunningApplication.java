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

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hoya.HoyaExitCodes;
import org.apache.hoya.api.HoyaClusterProtocol;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.yarn.appmaster.rpc.RpcBinder;
import org.apache.hoya.yarn.client.HoyaYarnClientImpl;

import static org.apache.hoya.Constants.*;

import java.io.IOException;

/**
 * A running application built from an app report. This one
 * can be talked to
 */
public class RunningApplication extends LaunchedApplication {

  private final ApplicationReport applicationReport;
  public RunningApplication(HoyaYarnClientImpl yarnClient,
                            ApplicationReport applicationReport) {
    super(yarnClient, applicationReport);
    this.applicationReport = applicationReport;
  }

  public ApplicationReport getApplicationReport() {
    return applicationReport;
  }


  /**
   * Connect to a Hoya AM
   * @param app application report providing the details on the application
   * @return an instance
   * @throws YarnException
   * @throws IOException
   */
  public HoyaClusterProtocol connect(ApplicationReport app) throws
                                                             YarnException,
                                                             IOException {

    try {
      return RpcBinder.getProxy(yarnClient.getConfig(),
                                yarnClient.getRmClient(),
                                app,
                                CONNECT_TIMEOUT,
                                RPC_TIMEOUT);
    } catch (InterruptedException e) {
      throw new HoyaException(HoyaExitCodes.EXIT_TIMED_OUT,
                              e,
                              "Interrupted waiting for communications with the HoyaAM");
    }
  }

}
