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

package org.apache.hadoop.hoya.yarn.appmaster

import groovy.util.logging.Commons
import org.apache.hadoop.hoya.tools.ConfigHelper
import org.apache.hadoop.yarn.service.AbstractService
import org.apache.hadoop.yarn.service.launcher.RunService

@Commons
class HoyaRegionService extends AbstractService
    implements RunService {

  String[] argv;

  HoyaRegionService() {
    super("HoyaRegionService")
    new ConfigHelper()
  }

  @Override
  void setArgs(String[] args) {
    this.argv = args;
  }


  @Override
  int runService() throws Throwable {
    HoyaRegionServiceArgs serviceArgs = new HoyaRegionServiceArgs(argv)
    serviceArgs.parse()
    serviceArgs.postProcess()

    //TODO: Install and run HBase
    return 0
  }
}


