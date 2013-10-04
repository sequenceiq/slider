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

package org.apache.hadoop.hoya.yarn.appmaster;

import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hoya.HoyaExitCodes;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.service.launcher.LauncherExitCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class AMUtils {
  /**
   * Map an exit code from a process 
   * @param exitCode
   * @return an exit code
   */
  public static int mapProcessExitCodeToYarnExitCode(int exitCode) {
    switch (exitCode) {
      case LauncherExitCodes.EXIT_SUCCESS:
        return LauncherExitCodes.EXIT_SUCCESS;
      //remap from a planned shutdown to a failure
      case LauncherExitCodes.EXIT_CLIENT_INITIATED_SHUTDOWN:
        return HoyaExitCodes.EXIT_MASTER_PROCESS_FAILED;
      default:
        return exitCode;
    }
  }


  /**
   * Map from a container to a role key by way of its priority
   * @param container
   * @return role key
   */
  public static int getRoleKey(Container c) {
    return c.getPriority().getPriority();
  }

}
