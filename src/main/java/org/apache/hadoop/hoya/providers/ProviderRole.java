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

package org.apache.hadoop.hoya.providers;

/**
 * Provider role and key for use in app requests.
 * 
 * This class uses the role name as the key for hashes and in equality tests,
 * and ignores the other values.
 */
public final class ProviderRole {
  public final String name;
  public final int id;
  public final boolean excludeFromFlexing; 

  public ProviderRole(String name, int id) {
    this(name, id, false);
  }

  public ProviderRole(String name, int id, boolean excludeFromFlexing) {
    this.name = name;
    this.id = id;
    this.excludeFromFlexing = excludeFromFlexing;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProviderRole that = (ProviderRole) o;
    if (!name.equals(that.name)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return "ProviderRole{" +
           "name='" + name + '\'' +
           ", key=" + id +
           ", excludeFromFlexing=" + excludeFromFlexing +
           '}';
  }
}
