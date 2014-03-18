#!/usr/bin/env python2.6

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

import logging
import os
import getpass
import platform
import re
import shlex
import socket
import multiprocessing
import subprocess

import time
import uuid
import hostname

log = logging.getLogger()

# selinux command
GET_SE_LINUX_ST_CMD = "/usr/sbin/sestatus"
GET_IFCONFIG_CMD = "ifconfig"
GET_UPTIME_CMD = "cat /proc/uptime"
GET_MEMINFO_CMD = "cat /proc/meminfo"


class Facter():

  def __init__(self):
    self.DATA_IFCONFIG_OUTPUT = Facter.setDataIfConfigOutput()

  @staticmethod
  def setDataIfConfigOutput():
    try:
      result = os.popen(GET_IFCONFIG_CMD).read()
      return result
    except OSError:
      log.warn("Can't execute {0}".format(GET_IFCONFIG_CMD))
    return ""

  # Returns the currently running user id
  def getId(self):
    return getpass.getuser()

  # Returns the FQDN of the host
  def getFqdn(self):
    return socket.getfqdn()

  # Returns the host's primary DNS domain name
  def getDomain(self):
    fqdn = self.getFqdn()
    hostname = self.getHostname()
    domain = fqdn.replace(hostname, "", 1)
    domain = domain.replace(".", "", 1)
    return domain

  # Returns the short hostname
  def getHostname(self):
    return hostname.hostname()

  # Returns the CPU hardware architecture
  def getArchitecture(self):
    result = platform.processor()
    if result == '':
      return 'OS NOT SUPPORTED'
    else:
      return result

  # Returns the name of the OS
  def getOperatingSystem(self):
    dist = platform.linux_distribution()
    operatingSystem = dist[0].lower()

    if os.path.exists('/etc/oracle-release'):
      return 'OracleLinux'
    elif operatingSystem.startswith('suse linux enterprise server'):
      return 'SLES'
    elif operatingSystem.startswith('red hat enterprise linux server'):
      return 'RedHat'
    elif operatingSystem != '':
      return operatingSystem
    else:
      return 'OS NOT SUPPORTED'

  # Returns the OS version
  def getOperatingSystemRelease(self):
    dist = platform.linux_distribution()
    if dist[1] != '':
      return dist[1]
    else:
      return 'OS NOT SUPPORTED'

  # Returns the OS TimeZone
  def getTimeZone(self):
    return time.tzname[time.daylight - 1]

  def getMacAddress(self):
    mac = uuid.getnode()
    if uuid.getnode() == mac:
      mac = ':'.join(
        '%02X' % ((mac >> 8 * i) & 0xff) for i in reversed(xrange(6)))
    else:
      mac = 'UNKNOWN'
    return mac

  # Returns the operating system family

  def getOsFamily(self):
    os_family = self.getOperatingSystem().lower()
    if os_family in ['redhat', 'fedora', 'centos', 'oraclelinux', 'ascendos',
                     'amazon', 'xenserver', 'oel', 'ovs', 'cloudlinux',
                     'slc', 'scientific', 'psbm']:
      os_family = 'RedHat'
    elif os_family in ['ubuntu', 'debian']:
      os_family = 'Debian'
    elif os_family in ['sles', 'sled', 'opensuse', 'suse']:
      os_family = 'Suse'
    elif os_family == '':
      os_family = 'OS NOT SUPPORTED'
    else:
      os_family = self.getOperatingSystem()
    return os_family

  def isSeLinux(self):

    try:
      retcode, out, err = run_os_command(GET_SE_LINUX_ST_CMD)
      se_status = re.search('(enforcing|permissive|enabled)', out)
      if se_status:
        return True
    except OSError:
      log.warn("Could not run {0}: OK".format(GET_SE_LINUX_ST_CMD))
    return False

  # Function that returns list of values that matches
  # Return empty str if no matches
  def data_return_list(self, patern, data):
    full_list = re.findall(patern, data)
    result = ""
    for i in full_list:
      result = result + i + ","

    result = re.sub(r',$', "", result)
    return result

  def data_return_first(self, patern, data):
    full_list = re.findall(patern, data)
    result = ""
    if full_list:
      result = full_list[0]

    return result

  #Convert kB to GB
  def convertSizeKbToGb(self, size):
    return "%0.2f GB" % round(float(size) / (1024.0 * 1024.0), 2)

  # Return first ip adress
  def getIpAddress(self):
    result = self.data_return_first(
      "(?: inet addr:)(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})",
      self.DATA_IFCONFIG_OUTPUT)
    if result == '':
      log.warn(
        "Can't get an ip address from {0}".format(self.DATA_IFCONFIG_OUTPUT))
      return socket.gethostbyname(socket.gethostname())
    else:
      return result

  # Return  netmask
  def getNetmask(self):
    result = self.data_return_first(
      "(?: Mask:)(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})",
      self.DATA_IFCONFIG_OUTPUT)
    if result == '':
      log.warn("Can't get a netmask from {0}".format(self.DATA_IFCONFIG_OUTPUT))
      return 'OS NOT SUPPORTED'
    else:
      return result

  # Return interfaces
  def getInterfaces(self):
    result = self.data_return_list("(\w+)(?:.*Link encap:)",
                                   self.DATA_IFCONFIG_OUTPUT)
    if result == '':
      log.warn("Can't get a network interfaces list from {0}".format(
        self.DATA_IFCONFIG_OUTPUT))
      return 'OS NOT SUPPORTED'
    else:
      return result


  def facterInfo(self):
    facterInfo = {}
    facterInfo['id'] = self.getId()
    facterInfo['domain'] = self.getDomain()
    facterInfo['fqdn'] = self.getFqdn()
    facterInfo['hostname'] = self.getHostname()
    facterInfo['macaddress'] = self.getMacAddress()
    facterInfo['operatingsystem'] = self.getOperatingSystem()
    facterInfo['operatingsystemrelease'] = self.getOperatingSystemRelease()
    facterInfo['timezone'] = self.getTimeZone()
    facterInfo['osfamily'] = self.getOsFamily()
    facterInfo['selinux'] = self.isSeLinux()

    facterInfo['ipaddress'] = self.getIpAddress()
    facterInfo['netmask'] = self.getNetmask()
    facterInfo['interfaces'] = self.getInterfaces()

    return facterInfo


def run_os_command(cmd):
  if type(cmd) == str:
    cmd = shlex.split(cmd)
  process = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stdin=subprocess.PIPE,
                             stderr=subprocess.PIPE
  )
  (stdoutdata, stderrdata) = process.communicate()
  return process.returncode, stdoutdata, stderrdata


def main(argv=None):
  print Facter().facterInfo()


if __name__ == '__main__':
  main()





