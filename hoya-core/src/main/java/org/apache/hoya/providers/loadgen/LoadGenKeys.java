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

package org.apache.hoya.providers.loadgen;

/**
 * Any keys related to load generation
 */
public interface LoadGenKeys {

  String ROLE_MASTER = "master";
  String ROLE_IOLOAD = "ioload";
  String ROLE_CPULOAD = "cpuload";
  String ROLE_FAILING = "failing";
  String ROLE_GENERAL1 = "general1";
  String ROLE_GENERAL2 = "general2";

  String KEY_SLEEPTIME = "load.sleeptime";

  String DEFAULT_SLEEPTIME = "10";
  
  /**
  work time, set to -1 or less for "forever":
   */
  String KEY_WORKTIME = "load.worktime";
  
  String DEFAULT_WORKTIME = "1";

  String KEY_LIFETIME = "load.lifetime";
  String DEFAULT_LIFETIME = "-1";

  String KEY_P_EXIT = "load.pfail";
  String DEFAULT_P_EXIT = "0";
  String KEY_EXITCODE = "load.exitcode";
  String DEFAULT_EXITCODE = "0";

  String KEY_READHEAVY = "load.writeheavy";
  String KEY_WRITEHEAVY = "load.writeheavy";
  String KEY_SEEKHEAVY = "load.seekheavy";
  String KEY_CPUHEAVY = "load.cpuheavy";

  String DEFAULT_MASTER_HEAP = "256";
  String DEFAULT_MASTER_YARN_RAM = "384";
  String DEFAULT_MASTER_YARN_VCORES = "1";
  String DEFAULT_ROLE_YARN_VCORES = "1";
  String DEFAULT_ROLE_HEAP = DEFAULT_MASTER_HEAP;
  String DEFAULT_ROLE_YARN_RAM = DEFAULT_MASTER_YARN_RAM;

  String PROVIDER_NAME = "loadgen";
}
