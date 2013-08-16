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

package org.apache.hadoop.hoya.api;

/**
 * Standard options for roles
 */
public interface StandardRoleOptions {

  String YARN_APP_RETRIES = "yarn.app.retries";
  String YARN_CORES = "yarn.vcores";
  String YARN_MEMORY = "yarn.memory";
  String YARN_PRIORITY = "yarn.priority";
  
  String APP_INFOPORT = "app.infoport";

  int DEF_YARN_REQUEST_PRIORITY = 0;
  int DEF_YARN_CORES = 1;
  int DEF_YARN_MEMORY = 256;
  int DEF_YARN_APP_RETRIES = 8;
}
