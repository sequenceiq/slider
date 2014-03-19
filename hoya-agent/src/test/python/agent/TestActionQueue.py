#!/usr/bin/env python

'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
from Queue import Queue

from unittest import TestCase
from agent.LiveStatus import LiveStatus
from agent.ActionQueue import ActionQueue
from agent.AgentConfig import AgentConfig
import os, errno, time, pprint, tempfile, threading
import StringIO
import sys
from threading import Thread

from mock.mock import patch, MagicMock, call
from agent.StackVersionsFileHandler import StackVersionsFileHandler
from agent.CustomServiceOrchestrator import CustomServiceOrchestrator
from agent.PythonExecutor import PythonExecutor
from agent.CommandStatusDict import CommandStatusDict


class TestActionQueue(TestCase):

  def setUp(self):
    out = StringIO.StringIO()
    sys.stdout = out
    # save original open() method for later use
    self.original_open = open


  def tearDown(self):
    sys.stdout = sys.__stdout__

  datanode_install_command = {
    'commandType': 'EXECUTION_COMMAND',
    'role': u'DATANODE',
    'roleCommand': u'INSTALL',
    'commandId': '1-1',
    'taskId': 3,
    'clusterName': u'cc',
    'serviceName': u'HDFS',
    'configurations':{'global' : {}},
    'configurationTags':{'global' : { 'tag': 'v1' }}
  }

  datanode_upgrade_command = {
      'commandId': 17,
      'role' : "role",
      'taskId' : "taskId",
      'clusterName' : "clusterName",
      'serviceName' : "serviceName",
      'roleCommand' : 'UPGRADE',
      'hostname' : "localhost.localdomain",
      'hostLevelParams': "hostLevelParams",
      'clusterHostInfo': "clusterHostInfo",
      'commandType': "EXECUTION_COMMAND",
      'configurations':{'global' : {}},
      'roleParams': {},
      'commandParams' :	{
        'source_stack_version' : 'HDP-1.2.1',
        'target_stack_version' : 'HDP-1.3.0'
      }
    }

  namenode_install_command = {
    'commandType': 'EXECUTION_COMMAND',
    'role': u'NAMENODE',
    'roleCommand': u'INSTALL',
    'commandId': '1-1',
    'taskId': 4,
    'clusterName': u'cc',
    'serviceName': u'HDFS',
    }

  snamenode_install_command = {
    'commandType': 'EXECUTION_COMMAND',
    'role': u'SECONDARY_NAMENODE',
    'roleCommand': u'INSTALL',
    'commandId': '1-1',
    'taskId': 5,
    'clusterName': u'cc',
    'serviceName': u'HDFS',
    }

  nagios_install_command = {
    'commandType': 'EXECUTION_COMMAND',
    'role': u'NAGIOS',
    'roleCommand': u'INSTALL',
    'commandId': '1-1',
    'taskId': 6,
    'clusterName': u'cc',
    'serviceName': u'HDFS',
    }

  hbase_install_command = {
    'commandType': 'EXECUTION_COMMAND',
    'role': u'HBASE',
    'roleCommand': u'INSTALL',
    'commandId': '1-1',
    'taskId': 7,
    'clusterName': u'cc',
    'serviceName': u'HDFS',
    }

  status_command = {
    "serviceName" : 'HDFS',
    "commandType" : "STATUS_COMMAND",
    "clusterName" : "",
    "componentName" : "DATANODE",
    'configurations':{}
  }

  datanode_restart_command = {
    'commandType': 'EXECUTION_COMMAND',
    'role': u'DATANODE',
    'roleCommand': u'CUSTOM_COMMAND',
    'commandId': '1-1',
    'taskId': 9,
    'clusterName': u'cc',
    'serviceName': u'HDFS',
    'configurations':{'global' : {}},
    'configurationTags':{'global' : { 'tag': 'v123' }},
    'hostLevelParams':{'custom_command': 'RESTART'}
  }

  @patch.object(ActionQueue, "process_command")
  @patch.object(Queue, "get")
  @patch.object(CustomServiceOrchestrator, "__init__")
  def test_ActionQueueStartStop(self, CustomServiceOrchestrator_mock,
                                get_mock, process_command_mock):
    CustomServiceOrchestrator_mock.return_value = None
    dummy_controller = MagicMock()
    config = MagicMock()
    actionQueue = ActionQueue(config, dummy_controller)
    actionQueue.start()
    time.sleep(0.1)
    actionQueue.stop()
    actionQueue.join()
    self.assertEqual(actionQueue.stopped(), True, 'Action queue is not stopped.')
    self.assertTrue(process_command_mock.call_count > 1)


  @patch("traceback.print_exc")
  @patch.object(ActionQueue, "execute_command")
  @patch.object(ActionQueue, "execute_status_command")
  def test_process_command(self, execute_status_command_mock,
                           execute_command_mock, print_exc_mock):
    dummy_controller = MagicMock()
    actionQueue = ActionQueue(AgentConfig("", ""), dummy_controller)
    execution_command = {
      'commandType' : ActionQueue.EXECUTION_COMMAND,
    }
    status_command = {
      'commandType' : ActionQueue.STATUS_COMMAND,
    }
    wrong_command = {
      'commandType' : "SOME_WRONG_COMMAND",
    }
    # Try wrong command
    actionQueue.process_command(wrong_command)
    self.assertFalse(execute_command_mock.called)
    self.assertFalse(execute_status_command_mock.called)
    self.assertFalse(print_exc_mock.called)

    execute_command_mock.reset_mock()
    execute_status_command_mock.reset_mock()
    print_exc_mock.reset_mock()
    # Try normal execution
    actionQueue.process_command(execution_command)
    self.assertTrue(execute_command_mock.called)
    self.assertFalse(execute_status_command_mock.called)
    self.assertFalse(print_exc_mock.called)

    execute_command_mock.reset_mock()
    execute_status_command_mock.reset_mock()
    print_exc_mock.reset_mock()

    actionQueue.process_command(status_command)
    self.assertFalse(execute_command_mock.called)
    self.assertTrue(execute_status_command_mock.called)
    self.assertFalse(print_exc_mock.called)

    execute_command_mock.reset_mock()
    execute_status_command_mock.reset_mock()
    print_exc_mock.reset_mock()

    # Try exception to check proper logging
    def side_effect(self):
      raise Exception("TerribleException")
    execute_command_mock.side_effect = side_effect
    actionQueue.process_command(execution_command)
    self.assertTrue(print_exc_mock.called)

    print_exc_mock.reset_mock()

    execute_status_command_mock.side_effect = side_effect
    actionQueue.process_command(execution_command)
    self.assertTrue(print_exc_mock.called)


  @patch.object(ActionQueue, "status_update_callback")
  @patch.object(ActionQueue, "determine_command_format_version")
  @patch.object(StackVersionsFileHandler, "read_stack_version")
  @patch.object(CustomServiceOrchestrator, "requestComponentStatus")
  @patch.object(ActionQueue, "execute_command")
  @patch.object(LiveStatus, "build")
  @patch.object(CustomServiceOrchestrator, "__init__")
  def test_execute_status_command(self, CustomServiceOrchestrator_mock,
                                  build_mock, execute_command_mock,
                                  requestComponentStatus_mock, read_stack_version_mock,
                                  determine_command_format_version_mock,
                                  status_update_callback):
    CustomServiceOrchestrator_mock.return_value = None
    dummy_controller = MagicMock()
    actionQueue = ActionQueue(AgentConfig("", ""), dummy_controller)

    build_mock.return_value = "dummy report"
    # Check execution ov V1 status command
    determine_command_format_version_mock.return_value = ActionQueue.COMMAND_FORMAT_V1
    actionQueue.execute_status_command(self.status_command)
    report = actionQueue.result()
    expected = 'dummy report'
    self.assertEqual(len(report['componentStatus']), 1)
    self.assertEqual(report['componentStatus'][0], expected)
    self.assertFalse(requestComponentStatus_mock.called)

    # Check execution ov V2 status command
    requestComponentStatus_mock.reset_mock()
    determine_command_format_version_mock.return_value = ActionQueue.COMMAND_FORMAT_V2
    actionQueue.execute_status_command(self.status_command)
    report = actionQueue.result()
    expected = 'dummy report'
    self.assertEqual(len(report['componentStatus']), 1)
    self.assertEqual(report['componentStatus'][0], expected)
    self.assertTrue(requestComponentStatus_mock.called)


  @patch.object(CustomServiceOrchestrator, "__init__")
  def test_determine_command_format_version(self,
                                            CustomServiceOrchestrator_mock):
    CustomServiceOrchestrator_mock.return_value = None
    v1_command = {
      'commandParams': {
        'schema_version': '1.0'
      }
    }
    v2_command = {
      'commandParams': {
        'schema_version': '2.0'
      }
    }
    current_command = {
      # Absent 'commandParams' section
    }
    dummy_controller = MagicMock()
    actionQueue = ActionQueue(AgentConfig("", ""), dummy_controller)
    self.assertEqual(actionQueue.determine_command_format_version(v1_command),
                     ActionQueue.COMMAND_FORMAT_V1)
    self.assertEqual(actionQueue.determine_command_format_version(v2_command),
                     ActionQueue.COMMAND_FORMAT_V2)
    self.assertEqual(actionQueue.determine_command_format_version(current_command),
                     ActionQueue.COMMAND_FORMAT_V1)