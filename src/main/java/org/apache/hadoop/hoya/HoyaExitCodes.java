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

package org.apache.hadoop.hoya;

import org.apache.hadoop.yarn.service.launcher.LauncherExitCodes;

public interface HoyaExitCodes extends LauncherExitCodes {

  /**
   * starting point for exit codes; not an exception itself
   */
  int _EXIT_CODE_BASE = 50;
  /**
   * internal error: {@value}
   */
  int EXIT_INTERNAL_ERROR = _EXIT_CODE_BASE;
  /**
   * Unimplemented feature: {@value}
   */
  int EXIT_UNIMPLEMENTED = 51;

  /**
   * service entered the failed state: {@value}
   */
  int EXIT_YARN_SERVICE_FAILED = 52;

  /**
   * service was killed: {@value}
   */
  int EXIT_YARN_SERVICE_KILLED = 53;

  /**
   * timeout on monitoring client: {@value}
   */
  int EXIT_TIMED_OUT = 54;

  /**
   * service finished with an error: {@value}
   */
  int EXIT_YARN_SERVICE_FINISHED_WITH_ERROR = 55;

  /**
   * the cluster is unknown: {@value}
   */
  int EXIT_UNKNOWN_HOYA_CLUSTER = 56;

  /**
   * the cluster is in the wrong state for that operation: {@value}
   */
  int EXIT_BAD_CLUSTER_STATE = 57;


}