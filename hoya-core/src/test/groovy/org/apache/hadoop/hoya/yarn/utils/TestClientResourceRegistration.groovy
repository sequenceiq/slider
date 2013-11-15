/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *   
 *    http://www.apache.org/licenses/LICENSE-2.0
 *   
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License. See accompanying LICENSE file.
 */

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

package org.apache.hadoop.hoya.yarn.utils

import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hoya.tools.ConfigHelper
import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.junit.Test

@Slf4j
class TestClientResourceRegistration {

  /**
   * Origin of a hoya resource -again, internal tracking 
   * rather than something to set by hand.
   */
  private final static String KEY_HOYA_RESOURCE_ORIGIN = "hoya.client.resource.origin";
  
  @Test
  public void testRegistration() throws Throwable {
    assert HoyaUtils.registerHoyaClientResource();
  }

  @Test
  public void testLoad() throws Throwable {
    assert HoyaUtils.registerHoyaClientResource();
    Configuration conf = new Configuration(true);
    assert conf.get(KEY_HOYA_RESOURCE_ORIGIN) == "test/resources"
  }

  @Test
  public void testMergeConfigs() throws Throwable {
    Configuration conf1 = new Configuration(false)
    conf1.set("key1", "conf1")
    conf1.set("key2", "conf1")
    Configuration conf2 = new Configuration(false)
    conf1.set("key1", "conf2")
    conf1.set("key3", "conf2")
    ConfigHelper.mergeConfigurations(conf1, conf2, "test")
    log.info(ConfigHelper.dumpConfigToString(conf1))

    assert conf1.get("key1").equals("conf2")
    assert conf1.get("key2").equals("conf1")
    assert conf1.get("key3").equals("conf2")
  }

  /**
   * This tests the situation where a yarn-config creation forces
   * a load of the default resources, which would overwrite any other
   * resources already in the list.
   * @throws Throwable
   */
  @Test
  public void testLoadRes() throws Throwable {
    Configuration conf = HoyaUtils.loadHoyaClientConfigurationResource()
    assert conf.get(KEY_HOYA_RESOURCE_ORIGIN) == "test/resources"
    String hostname = "nosuchhost:0"
    conf.set(YarnConfiguration.RM_ADDRESS, hostname)
    YarnConfiguration yc = new YarnConfiguration()
    ConfigHelper.mergeConfigurations(yc, conf, "hoya-client")
    InetSocketAddress addr = HoyaUtils.getRmAddress(yc)
    assert HoyaUtils.isAddressDefined(addr)


  }


}
