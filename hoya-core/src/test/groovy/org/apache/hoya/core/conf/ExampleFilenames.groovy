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

package org.apache.hoya.core.conf

/*
  names of the example configs
 */

class ExampleFilenames {

  static final String overridden = "example-overridden.json"
  static final String overriddenRes = "example-overridden-resolved.json"
  static final String internal = "example-internal.json"
  static final String internalRes = "example-internal-resolved.json"
  static final String app_configuration = "example-app_configuration.json"
  final
  static String app_configurationRes = "example-app_configuration-resolved.json"
  static final String resources = "example-resources.json"
  static final String empty = "example-empty.json"

  static final String PACKAGE = "org.apache.hoya.core.conf.examples"

  
  static final String[] all_examples = [overridden, overriddenRes, internal, internalRes,
                                      app_configuration, app_configurationRes, resources, empty];

  static final List<String> all_example_resources = [];
  static {
    all_examples.each { all_example_resources << (PACKAGE + "." + it) }
  }

}
