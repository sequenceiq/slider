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

package org.apache.hoya.yarn.params;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.apache.hoya.yarn.HoyaActions;


@Parameters(commandNames = {HoyaActions.ACTION_CREATE},
            commandDescription = HoyaActions.DESCRIBE_ACTION_CREATE)

public class HoyaAMCreateAction extends AbstractActionArgs implements
                                                           LaunchArgsAccessor {


  @Override
  public String getActionName() {
    return HoyaActions.ACTION_CREATE;
  }

  @Parameter(names = ARG_IMAGE, description = "image", required = false)
  public String image;

  /**
   * This is the URI in the FS to the Hoya cluster; the conf file (and any
   * other cluster-specifics) can be picked up here
   */
  @Parameter(names = ARG_HOYA_CLUSTER_URI,
             description = "URI to the hoya cluster", required = true)
  public String hoyaClusterURI;

  @ParametersDelegate
  LaunchArgsDelegate launchArgs = new LaunchArgsDelegate();

  @Override
  public String getRmAddress() {
    return launchArgs.getRmAddress();
  }

  @Override
  public int getWaittime() {
    return launchArgs.getWaittime();
  }

  @Override
  public void setWaittime(int waittime) {
    launchArgs.setWaittime(waittime);
  }
  
}
