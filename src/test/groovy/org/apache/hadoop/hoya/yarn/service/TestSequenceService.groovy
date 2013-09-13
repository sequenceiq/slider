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

package org.apache.hadoop.hoya.yarn.service

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.service.Service
import org.apache.hadoop.yarn.service.launcher.ServiceLauncherBaseTest
import org.junit.Test

class TestSequenceService extends ServiceLauncherBaseTest {


  @Test
  public void testSingleSequence() throws Throwable {
    SequenceService ss = startService([new MockService()])
    ss.stop();
  }


  @Test
  public void testSequence() throws Throwable {
    MockService one = new MockService("one", false, 100)
    MockService two = new MockService("two", false, 100)
    SequenceService ss = startService([one, two])
    assert ss.waitForServiceToStop(1000);
    assert one.isInState(Service.STATE.STOPPED)
    assert two.isInState(Service.STATE.STOPPED)
  }

  @Test
  public void testFailingSequence() throws Throwable {
    MockService one = new MockService("one", true, 100)
    MockService two = new MockService("two", false, 100)
    SequenceService ss = startService([one, two])
    assert ss.waitForServiceToStop(1000);
    assert one.isInState(Service.STATE.STOPPED)
    assert two.isInState(Service.STATE.NOTINITED)
  }


  @Test
  public void testFailInStartNext() throws Throwable {
    MockService one = new MockService("one", false, 100)
    MockService two = new MockService("two", true, 0)
    MockService three = new MockService("3", false, 0)
    SequenceService ss = startService([one, two, three])
    assert ss.waitForServiceToStop(1000);
    assert one.isInState(Service.STATE.STOPPED)
    assert two.isInState(Service.STATE.STOPPED)
    Throwable failureCause = two.failureCause
    assert failureCause != null;
    Throwable masterFailureCause = ss.failureCause
    assert masterFailureCause != null;
    assert masterFailureCause == failureCause

    assert three.isInState(Service.STATE.NOTINITED)
  }





  public SequenceService startService(List<MockService> services) {
    SequenceService ss = new SequenceService("test")
    services.each { ss.addService(it) }
    ss.init(new Configuration())
    //expect service to start and stay started
    ss.start();
    return ss
  }


}
