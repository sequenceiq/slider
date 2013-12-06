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

package org.apache.hadoop.hoya.yarn.params;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException;

import java.util.Map;

public class ActionFlex extends AbstractAction {


  @ParametersDelegate
  public RoleDelegate roleDelegate;

  @Parameter(names = {ARG_PERSIST},
             description = "flag to indicate whether a flex change should be persisted (default=true)",
             arity = 1)
  public boolean persist;

  /**
   * Get the role mapping (may be empty, but never null)
   * @return role mapping
   * @throws BadCommandArgumentsException parse problem
   */
  public Map<String, String> getRoleMap() throws BadCommandArgumentsException {
    return roleDelegate.getRoleMap();
  }
}
