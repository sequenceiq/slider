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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Execute a given application
 */
public class RunHBase implements Runnable {
  Log LOG = LogFactory.getLog(RunHBase.class);
  private final ProcessBuilder builder;
  private Process process;
  private Exception exception;
  private Integer exitCode = null;

  public RunHBase(String... commands) {
    builder = new ProcessBuilder(commands);
  }
  
  public RunHBase(List<String> commands) {
    builder = new ProcessBuilder(commands);
  }

  public ProcessBuilder getBuilder() {
    return builder;
  }
  
  public void setEnv(String key, String val) {
    builder.environment().put(key, val);
  }

  /**
   * Bulk set the environment from a map. This does
   * not replace the existing environment, just extend it/overwrite single
   * entries.
   * @param map map to add
   */
  public void setEnv(Map<String, String>map) {
    for (Map.Entry<String, String> entry: map.entrySet()) {
      setEnv(entry.getKey(), entry.getValue());
    }
  }
  
  public String getEnv(String key) {
    return builder.environment().get(key);
  }

  public Process getProcess() {
    return process;
  }

  public Exception getException() {
    return exception;
  }

  public List<String> getCommands() {
    return builder .command();
  }
  
  public String getCommand() {
    return getCommands().get(0);
  }
  

  /**
   * Exec the process
   * @return the process
   * @throws IOException
   */
  public Process exec() throws IOException {
    if (process!=null) {
      throw new IllegalStateException("Process already started");
    }
    process = builder.start();
    return process;
  }

  
  /**
   * Entry point for waiting for the program to finish
   */
  //@Override // Runnable
  public void run() {
    try {
      exitCode = process.waitFor();
    } catch (InterruptedException e) {
      LOG.debug("Process wait interrupted -exiting thread");
    }
  }

  /**
   * Create a thread to wait for this command to complete.
   * THE THREAD IS NOT STARTED.
   * @return the thread
   * @throws IOException Execution problems
   */
  public Thread execIntoThread() throws IOException {
    exec();
    Thread thread = new Thread(this, getCommand());
    return thread;
  }
}
