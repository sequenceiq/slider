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

package org.apache.hadoop.hoya.yarn.appmaster;

import org.apache.hadoop.hoya.providers.ProviderRole;

/**
 * Models the ongoing status of a role. 
 * Nothing here is synchronized: grab the whole instance to update.
 */
public class RoleStatus {


  private final String name;


  /**
   * Role key in the container details stored in the AM,
   * currently mapped to priority
   */
  private final int key;
  
  private final boolean excludeFromFlexing;
  
  private int desired, actual, requested, releasing;
  private int failed, started, startFailed, completed;


  public RoleStatus(ProviderRole providerRole) {
    this.name = providerRole.name;
    this.key = providerRole.key;
    this.excludeFromFlexing = providerRole.excludeFromFlexing;
  }

  public RoleStatus(String name,
                    int key,
                    int desired,
                    int actual,
                    int requested,
                    int releasing) {
    this.name = name;
    this.key = key;
    this.desired = desired;
    this.actual = actual;
    this.requested = requested;
    this.releasing = releasing;
    excludeFromFlexing = false;
  }

  public String getName() {
    return name;
  }

  public int getKey() {
    return key;
  }

  public boolean getExcludeFromFlexing() {
    return excludeFromFlexing;
  }

  public int getDesired() {
    return desired;
  }

  public void setDesired(int desired) {
    this.desired = desired;
  }

  public int getActual() {
    return actual;
  }

  public void setActual(int actual) {
    this.actual = actual;
  }

  public int getRequested() {
    return requested;
  }

  public void setRequested(int requested) {
    this.requested = requested;
  }

  public int getReleasing() {
    return releasing;
  }

  public void setReleasing(int releasing) {
    this.releasing = releasing;
  }

  public int getFailed() {
    return failed;
  }

  public void setFailed(int failed) {
    this.failed = failed;
  }

  public int getStartFailed() {
    return startFailed;
  }

  public void setStartFailed(int startFailed) {
    this.startFailed = startFailed;
  }

  public int getCompleted() {
    return completed;
  }

  public void setCompleted(int completed) {
    this.completed = completed;
  }

  public int getStarted() {
    return started;
  }

  public void setStarted(int started) {
    this.started = started;
  }

  /**
   * Get the number of roles we are short of.
   * nodes released are ignored.
   * @return the positive or negative number of roles to add/release.
   * 0 means "do nothing".
   */
  public int getDelta() {
    int inuse = actual + requested;
    //don't know how to view these. Are they in-use or not?
    //inuse += releasing;
    int delta = desired - inuse;
    if (delta<0) {
      //if we are releasing, remove the number that are already released.
      delta += releasing;
      //but never switch to a positive
      delta = Math.min(delta, 0);
    }
    return delta;
  }

  @Override
  public String toString() {
    return "RoleStatus{" +
           "name='" + name + '\'' +
           ", key=" + key +
           ", desired=" + desired +
           ", actual=" + actual +
           ", requested=" + requested +
           ", releasing=" + releasing +
           ", failed=" + failed +
           ", started=" + started +
           ", startFailed=" + startFailed +
           ", completed=" + completed +
           '}';
  }
}
