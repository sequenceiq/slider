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

package org.apache.hadoop.hoya;


/**
 * Keys and various constants for Hoya
 */
public interface HoyaKeys {

  /**
   * The path under which cluster and temp data are stored
   * {@value}
   */
  String HOYA_BASE_DIRECTORY = ".hoya";

  /**
   *  name of the relative path to install hBase into:  {@value}
   */
  String HBASE_LOCAL = "hbaselocal";


  /**
   * Application type for YARN  {@value}
   */
  String APP_TYPE = "hoya";

  /**
   * JVM arg to force IPv4  {@value}
   */
  String JAVA_FORCE_IPV4 = "-Djava.net.preferIPv4Stack=true";

  /**
   * JVM arg to go headless  {@value}
   */

  String JAVA_HEADLESS = "-Djava.awt.headless=true";


  /**
   * This is the name of the dir/subdir containing
   * the hbase conf that is propagated via YARN
   *  {@value}
   */
  String PROPAGATED_CONF_DIR_NAME = "conf";
  String GENERATED_CONF_DIR_NAME = "generated";
  String ORIG_CONF_DIR_NAME = "original";
  String HBASE_DATA_DIR_NAME = "hbase";
  String CLUSTER_SPECIFICATION_FILE = "cluster.json";

  int MIN_HEAP_SIZE = 0;
  String HBASE_LOG_DIR = "HBASE_LOG_DIR";

  String HOYA_XML ="org/apache/hadoop/hoya/hoya.xml";

  String HOYA_PROVIDER_KEY = "hoya.provider.%s";

  String KEY_HOYA_TEMPLATE_ORIGIN = "hoya.template.origin";
  
}
