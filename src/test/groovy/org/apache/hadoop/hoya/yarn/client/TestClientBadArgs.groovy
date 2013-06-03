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

package org.apache.hadoop.hoya.yarn.client

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncherBaseTest
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.junit.Test

/**
 * Test the argument parsing/validation logic
 */
class TestClientBadArgs extends ServiceLauncherBaseTest {
  @Test
  public void testNoAction() throws Throwable {
    launchExpectingException(HoyaClient, new Configuration(),
                             CommonArgs.ERROR_NO_ACTION)

  }

  @Test
  public void testUnknownAction() throws Throwable {
    launchExpectingException(HoyaClient, new Configuration(),
                             CommonArgs.ERROR_UNKNOWN_ACTION,
                             "not-a-known-action")
  }

  @Test
  public void testActionWithoutEnoughArgs() throws Throwable {
    launchExpectingException(HoyaClient, new Configuration(),
                             CommonArgs.ERROR_NOT_ENOUGH_ARGUMENTS,
                             ClientArgs.ACTION_START)
  }

  @Test
  public void testActionWithoutTooManyArgs() throws Throwable {
    launchExpectingException(HoyaClient, new Configuration(),
                             CommonArgs.ERROR_TOO_MANY_ARGUMENTS,
                             ClientArgs.ACTION_HELP,
                             "hello, world")
  }
  @Test
  public void testBadImageArg() throws Throwable {
    launchExpectingException(HoyaClient, new Configuration(),
                             "Expected a value after parameter",
                             ClientArgs.ACTION_HELP,
                             CommonArgs.ARG_IMAGE)
  }

}
