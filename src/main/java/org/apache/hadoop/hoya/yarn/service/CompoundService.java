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

package org.apache.hadoop.hoya.yarn.service;

import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;

/**
 * An extended composite service which not only makes the 
 * addService method public, it auto-registers
 * itself as a listener for state change events
 */
public class CompoundService extends CompositeService implements Parent,
                                                                 ServiceStateChangeListener {

  public CompoundService(String name) {
    super(name);
  }
  
  
  public CompoundService() {
    super("CompoundService");
  }

  /**
   * Add a service, and register it
   * @param service the {@link Service} to be added
   */
  @Override
  public void addService(Service service) {
    service.registerServiceListener(this);
    super.addService(service);
  }

  /**
   * When this service is started, any service stopping with a failure
   * exception is converted immediately into a failure of this service, 
   * storing the failure and stopping ourselves.
   * @param service the service that has changed.
   */
  @Override
  public void stateChanged(Service service) {
    //if that service stopped while we are running:
    if (isInState(STATE.STARTED) && service.isInState(STATE.STOPPED)) {
        //did the service fail? if so: propagate
        Throwable failureCause = service.getFailureCause();
        if (failureCause != null) {
          //failure. Convert to an exception
          Exception e = HoyaServiceUtils.convertToException(failureCause);
          //flip ourselves into the failed state
          noteFailure(e);
          stop();
        }
    }
  }
}
