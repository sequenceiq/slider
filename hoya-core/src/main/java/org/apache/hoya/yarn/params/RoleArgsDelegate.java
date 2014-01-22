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
import org.apache.hoya.exceptions.BadCommandArgumentsException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RoleArgsDelegate extends AbstractArgsDelegate {

  /**
   * This is a listing of the roles to create
   */
  @Parameter(names = {ARG_ROLE}, arity = 2,
             description = "role <name> <count>")
  public List<String> roleTuples = new ArrayList<String>(0);


  /**
   * Get the role mapping (may be empty, but never null)
   * @return role mapping
   * @throws BadCommandArgumentsException parse problem
   */
  public Map<String, String> getRoleMap() throws BadCommandArgumentsException {
    return convertTupleListToMap("roles", roleTuples);
  }

  public List<String> getRoleTuples() {
    return roleTuples;
  }
}
