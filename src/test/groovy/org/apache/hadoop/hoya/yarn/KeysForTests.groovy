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

package org.apache.hadoop.hoya.yarn

import org.apache.hadoop.hoya.HoyaKeys

/**
 * Keys shared across tests
 */
public interface KeysForTests extends HoyaKeys {
  /**
   * Username for all clusters, ZK, etc
   */
  String USERNAME = "bigdataborat"

  int WAIT_TIME = 120;
  String WAIT_TIME_ARG = WAIT_TIME.toString()

  String HOYA_TEST_HBASE_HOME = "hoya.test.hbase_home";
  String HOYA_TEST_HBASE_TAR = "hoya.test.hbase_tar";

  String HOYA_TEST_ACCUMULO_HOME = "hoya.test.accumulo.home";
  String HOYA_TEST_ACCUMULO_TAR = "hoya.test.accumulo.tar";

  String HOYA_TEST = "hoya-test.xml"
}