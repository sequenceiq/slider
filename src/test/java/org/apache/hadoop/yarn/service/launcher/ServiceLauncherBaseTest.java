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
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher;
import org.junit.Assert;

import java.util.List;

/**
 * Base class for tests that use the service launcher
 */
public class ServiceLauncherBaseTest extends Assert {

  /**
   * Launch a service
   * @param serviceClass service class
   * @param conf configuration
   * @param args list of args to hand down (as both raw and processed)
   * @return the service launcher. It's exitCode field will
   * contain any exit code; its <code>service</code> field
   * the service itself.
   */
  protected ServiceLauncher launch(Class serviceClass,
                                Configuration conf,
                                List<Object> args) throws
                                    Throwable {
    ServiceLauncher serviceLauncher =
      new ServiceLauncher(serviceClass.getName());
    serviceLauncher.launchService(conf,
                                  toArray(args),
                                  false);
    return serviceLauncher;
  }

  protected static String[] toArray(List<Object> args) {
    String[] converted = new String[args.size()];
    for (int i = 0; i < args.size(); i++) {
      converted[i] = args.get(i).toString();
    }
    return converted;
  }

  /**
   * Launch a service
   * @param serviceClass service class
   * @param conf configuration
   * @param args list of args to hand down (as both raw and processed)
   */
  protected void launchExpectingException(Class serviceClass,
                                       Configuration conf,
                                       String expectedText,
                                       List args) throws
                                                       Throwable {
    ServiceLauncher serviceLauncher =
      new ServiceLauncher(serviceClass.getName());
    try {
      int result = serviceLauncher.launchService(conf, toArray(args), false);
      fail("Expected an exception with text containing " + expectedText
           + " -but the service completed with exit code " + result);
    } catch (Throwable thrown) {
      if (!thrown.toString().contains(expectedText)) {
        //not the right exception -rethrow
        throw thrown;
      }
    }
  }


}

