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

import groovy.json.JsonOutput;
import groovy.transform.CompileStatic;
import groovy.util.logging.Slf4j;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Assume
import org.junit.Before;

/**
 * Base class for unit tests as well as ones starting mini clusters
 * -the foundational code and methods
 * 
 * 
 */

@CompileStatic
@Slf4j

public class HoyaTestBase extends Assert {
  protected static String[] toArray(List<Object> args) {
    String[] converted = new String[args.size()];
    for (int i = 0; i < args.size(); i++) {
      converted[i] = args.get(i).toString();
    }
    return converted;
  }

  protected YarnConfiguration createConfiguration() {
    return HoyaUtils.createConfiguration();
  }

  protected void describe(String s) {
    log.info("");
    log.info("===============================");
    log.info(s);
    log.info("===============================");
    log.info("");
  }

  String prettyPrint(String json) {
    JsonOutput.prettyPrint(json)
  }

  public void skip(String message) {
    Assume.assumeTrue(message, false);
  }

  @Before
  public void setup() {
    //give our thread a name
    Thread.currentThread().name = "JUnit"
  }

  public void assertListEquals(List left, List right) {
    assert left.size() == right.size();
    for (int i = 0; i < left.size(); i++) {
      assert left[0] == right[0]
    }
  }
}
