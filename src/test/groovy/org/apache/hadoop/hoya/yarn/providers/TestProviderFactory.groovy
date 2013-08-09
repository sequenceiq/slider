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

package org.apache.hadoop.hoya.yarn.providers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hoya.providers.HoyaProviderFactory
import org.apache.hadoop.hoya.providers.hbase.HBaseProviderFactory
import org.junit.Test

class TestProviderFactory {


  @Test
  public void testLoadHoyaConfig() throws Throwable {
    Configuration conf = HoyaProviderFactory.loadHoyaConfiguration();
    assert conf.getBoolean("hoya.config.loaded", false)
  }
  @Test
  public void testLoadHBaseProvider() throws Throwable {
    HoyaProviderFactory factory = HoyaProviderFactory.createHoyaProviderFactory(HBaseProviderFactory.APPLICATION_TYPE);
    assert factory instanceof HBaseProviderFactory
  }

  @Test
  public void testCreateHBaseProviderBuilder() throws Throwable {
    HoyaProviderFactory factory = HoyaProviderFactory.createHoyaProviderFactory(HBaseProviderFactory.APPLICATION_TYPE);
    assert null != factory.createBuilder();
  }

  @Test
  public void testCreateHBaseProviderDeployer() throws Throwable {
    HoyaProviderFactory factory = HoyaProviderFactory.createHoyaProviderFactory(HBaseProviderFactory.APPLICATION_TYPE);
    assert null != factory.createDeployer();
  }
  
  
}
