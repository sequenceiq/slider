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

package org.apache.hadoop.yarn.service.launcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.Service;

import java.io.IOException;

/**
 * An interface which services can implement to have their
 * execution managed by the ServiceLauncher.
 * The command line options will be passed down before the 
 * {@link Service#init(Configuration)} operation is invoked via an
 * invocation of {@link RunService#setArgs(String[])}
 * After the service has been successfully started via {@link Service#start()}
 * the {@link RunService#runService()} method is called to execute the 
 * service. When this method returns, the service launcher will exit, using
 * the return code from the method as its exit option.
 */
public interface RunService {

  /**
   * Propagate the command line arguments
   * 
   * @param args argument list
   * @throws IOException any problem
   */
  void setArgs(String[] args) throws IOException;
  
  /**
   * Run a service
   * @return the exit code
   * @throws Throwable any exception to report
   */
  int runService() throws Throwable ;
}
