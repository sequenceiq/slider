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

import groovy.transform.CompileStatic
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.junit.Assert
import org.junit.Test

/**
 * Test handling of common arguments, specifically how things get split up
 */
@CompileStatic
class TestCommonArgParsing {

  @Test
  public void testCreateActionArgs() throws Throwable {
    ClientArgs clientArgs = createClientArgs([ClientArgs.ACTION_CREATE, 'cluster1'])
    assert clientArgs.clusterName == 'cluster1'
  }

  @Test
  public void testCreateFailsNoClustername() throws Throwable {
    assertParseFails([ClientArgs.ACTION_CREATE])

  }

  @Test
  public void testCreateFailsTwoClusternames() throws Throwable {
    assertParseFails([
        ClientArgs.ACTION_CREATE,
        "c1",
        "c2",
      ])
  }


  @Test
  public void testListNoClusternames() throws Throwable {
    ClientArgs clientArgs = createClientArgs([ClientArgs.ACTION_LIST])
    assert clientArgs.clusterName == null
  }

  @Test
  public void testList1Clustername() throws Throwable {
    ClientArgs clientArgs = createClientArgs([ClientArgs.ACTION_LIST, 'cluster1'])
    assert clientArgs.clusterName == 'cluster1'
  }

  @Test
  public void testListFailsTwoClusternames() throws Throwable {
    assertParseFails([
        ClientArgs.ACTION_LIST,
        "c1",
        "c2",
      ])
  }

  
  private void assertParseFails(List argsList) {
    try {
      ClientArgs clientArgs = createClientArgs(argsList)
      Assert.fail("exected an exception, got $clientArgs")
    } catch (BadCommandArgumentsException expected) {
      //expected
    }
  }
  
  /**
   * build and parse client args, after adding the base args list
   * @param argsList
   */
  public ClientArgs createClientArgs(List argsList) {
    def serviceArgs = new ClientArgs(argsList + baseArgs())
    serviceArgs.parse()
    serviceArgs.postProcess();
    serviceArgs
  }

  /**
   * build the list of base arguments for all operations
   * @return the base arguments
   */
  private def baseArgs() {
    return [
        CommonArgs.ARG_ZKHOSTS, "localhost",
        CommonArgs.ARG_ZKPORT, "8080",
    ]
  }
}
