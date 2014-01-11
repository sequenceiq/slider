package org.apache.hoya.funtest.framework

import org.apache.hadoop.hoya.HoyaXMLConfKeysForTesting

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

public interface HoyaTestProperties extends HoyaXMLConfKeysForTesting {

  /**
   * Maven Property of location of hoya conf dir: {@value}
   */
  String HOYA_CONF_DIR_PROP = "hoya.conf.dir"

  /**
   * Maven Property of location of hoya binary image dir: {@value}
   */
  String HOYA_BIN_DIR_PROP = "hoya.bin.dir"
  
  String KEY_HOYA_THAW_WAIT_TIME = "hoya.test.thaw.wait.seconds"

  int DEFAULT_HOYA_THAW_WAIT_TIME = 60000

  String KEY_HOYA_FREEZE_WAIT_TIME = "hoya.test.freeze.wait.seconds"

  int DEFAULT_HOYA_FREEZE_WAIT_TIME = 60000

  String KEY_HOYA_TEST_ZK_HOSTS = "hoya.test.zkhosts";

}