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

package org.apache.hoya.funtest.itest

import org.apache.bigtop.itest.shell.Shell
import org.junit.BeforeClass
import org.apache.hadoop.conf.Configuration
import org.apache.bigtop.itest.JarContent

class HoyaCommandTestBase {
  private static String USER = System.getProperty("user.name")
  private static String pwd = ""
  private static Configuration conf
  private static Shell sh = new Shell('/bin/bash -s');
  public static final String HOYA_CONF_DIR = "hoya.conf.dir"
  public static final String HOYA_BIN_DIR = "hoya.bin.dir"


  @BeforeClass
  static void setupClass() {
    sh.exec("pwd")
    pwd = sh.out
    int lastIndex = pwd.length() - 1
    pwd = pwd.substring(1, lastIndex)

//    JarContent.unpackJarContainer(HoyaCommandTestBase, '.', null);

  }
  
  def hoya(String conf, Object...commands) {
    sh.exec("export HOYA_CONF_DIR=./${id}",
            "bin/hoya",
            commands);
  }
  
}
