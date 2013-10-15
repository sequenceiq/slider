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
 *  Keys for map entries in roles
 */
public interface OptionKeys {

  /**
   * option used on command line to set the test flag
   * {@value}
   */
  String OPTION_TEST = "hoya.test";

  /**
   * Prefix for site.xml options: {@value}
   */
  String OPTION_SITE_PREFIX = "site.";

  /**
   * Version of the app: {@value}
   */
  String OPTION_APP_VERSION = "cluster.app.version";

  /**
   * Time in milliseconds to wait after forking the in-AM master
   * process before attempting to start up the containers. 
   * A shorter value brings the cluster up faster, but means that if the
   * master process fails (due to a bad configuration), then time
   * is wasted starting containers on a cluster that isn't going to come
   * up
   */
  String OPTION_CONTAINER_STARTUP_DELAY = "hoya.container.startup.delay";

  /**
   * Version of the app: {@value}
   */
  String OPTION_KEYTAB_LOCATION = "cluster.keytab.location";


  String HOYA_CLUSTER_DIRECTORY_PERMISSIONS =
    "hoya.cluster.directory.permissions";
  String DEFAULT_HOYA_CLUSTER_DIRECTORY_PERMISSIONS =
    "0755";
  String HOYA_DATA_DIRECTORY_PERMISSIONS = "hoya.data.directory.permissions";
  String DEFAULT_HOYA_DATA_DIRECTORY_PERMISSIONS = "0755";
}
