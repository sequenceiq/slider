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

package org.apache.hadoop.hoya.yarn;

/**
 * Actions.
 * Only some of these are supported by specific Hoya Services; they
 * are listed here to ensure the names are consistent
 */
public interface HoyaActions {
  String ACTION_ADDNODE = "addnode";
  String ACTION_CREATE = "create";
  String ACTION_DESTROY = "destroy";
  String ACTION_GETSIZE = "getsize";
  String ACTION_FLEX = "flex";
  String ACTION_GETCONF = "getconf";
  String ACTION_HELP = "help";
  String ACTION_EXISTS = "exists";
  String ACTION_LIST = "list";
  String ACTION_MIGRATE = "migrate";
  String ACTION_PREFLIGHT = "preflight";
  String ACTION_RECONFIGURE = "reconfigure";
  String ACTION_REIMAGE = "reimage";
  String ACTION_START = "start";
  String ACTION_STATUS = "status";
  String ACTION_STOP = "stop";
}
