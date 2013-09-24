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

package org.apache.hadoop.hoya.yarn.appmaster.state;

import org.apache.hadoop.hoya.providers.ProviderRole;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A class that contains all the ongoing state of a Hoya AM.
 * Keeping it isolated aids in testing and synchronizing access
 */
public class AppState {


  private final Map<Integer, RoleStatus> roleStatusMap =
    new HashMap<Integer, RoleStatus>();

  private final Map<String, ProviderRole> roles =
    new HashMap<String, ProviderRole>();

  private final ContainerTracker containerTracker = new ContainerTracker();
  
  /**
   *  This is the number of containers which we desire for HoyaAM to maintain
   */
  //private int desiredContainerCount = 0;

  /**
   * Counter for completed containers ( complete denotes successful or failed )
   */
  private final AtomicInteger numCompletedContainers = new AtomicInteger();

  /**
   *   Count of failed containers

   */
  private final AtomicInteger numFailedContainers = new AtomicInteger();

  /**
   * # of started containers
   */
  private final AtomicInteger startedContainers = new AtomicInteger();

  /**
   * # of containers that failed to start 
   */
  private final AtomicInteger startFailedContainers = new AtomicInteger();


  public int getFailedCountainerCount() {
    return numFailedContainers.get();
  }

  /**
   * Increment the count and return the new value
   * @return the latest failed container count
   */
  public int incFailedCountainerCount() {
    return numFailedContainers.incrementAndGet();
  }

  public int getStartFailedCountainerCount() {
    return startFailedContainers.get();
  }

  /**
   * Increment the count and return the new value
   * @return the latest failed container count
   */
  public int incStartedCountainerCount() {
    return startedContainers.incrementAndGet();
  }

  public int getStartedCountainerCount() {
    return startedContainers.get();
  }

  /**
   * Increment the count and return the new value
   * @return the latest failed container count
   */
  public int incStartFailedCountainerCount() {
    return startFailedContainers.incrementAndGet();
  }

  public AtomicInteger getStartFailedContainers() {
    return startFailedContainers;
  }
  

  public Map<Integer, RoleStatus> getRoleStatusMap() {
    return roleStatusMap;
  }

  /**
   * Add knowledge of a role.
   * This is a build-time operation that is not synchronized, and
   * should be used while setting up the system state -before servicing
   * requests.
   * @param providerRole role to add
   */
  public void buildRole(ProviderRole providerRole) {
    //build role status map
    roleStatusMap.put(providerRole.key,
                      new RoleStatus(providerRole));
    roles.put(providerRole.name, providerRole);
  }


  /**
   * Look up a role from its key -or fail 
   *
   * @param key key to resolve
   * @return the status
   * @throws YarnRuntimeException on no match
   */
  public  RoleStatus lookupRoleStatus(int key) {
    RoleStatus rs = getRoleStatusMap().get(key);
    if (rs == null) {
      throw new YarnRuntimeException("Cannot find role for role key " + key);
    }
    return rs;
  }

  /**
   * Look up a role from its key -or fail 
   *
   * @param c container in a role
   * @return the status
   * @throws YarnRuntimeException on no match
   */
  public  RoleStatus lookupRoleStatus(String name) {
    ProviderRole providerRole = roles.get(name);
    if (providerRole == null) {
      throw new YarnRuntimeException("Unknown role " + name);
    }
    return lookupRoleStatus(providerRole.key);
  }
  
  /**
   * Get all the active containers
   */
  public ConcurrentMap<ContainerId, ContainerInfo> getActiveContainers() {
    return containerTracker.getActiveContainers();
  }

  /**
   * The containers we have released, but we
   * are still awaiting acknowledgements on. Any failure of these
   * containers is treated as a successful outcome
   */
  public  ConcurrentMap<ContainerId, Container> getContainersBeingReleased() {
    return containerTracker.getContainersBeingReleased();
  }


  /**
   * Return the percentage done that Hoya is to have YARN display in its
   * Web UI
   * @return an number from 0 to 100
   */
  public synchronized float getApplicationProgressPercentage() {
    float percentage = 0;
    int desired = 0;
    float actual = 0;
    for (RoleStatus role : getRoleStatusMap().values()) {
      desired += role.getDesired();
      actual += role.getActual();
    }
    if (desired == 0) {
      percentage = 100;
    } else {
      percentage = actual / desired;
    }
    return percentage;
  }
}
