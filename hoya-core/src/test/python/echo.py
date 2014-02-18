#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging, sys, datetime, time


def main():
  print "Executing echo"
  logfilename = "echo" + str(datetime.datetime.now()) + ".log"
  logging.basicConfig(filename=logfilename, level=logging.DEBUG)
  logging.debug('Starting echo script ...')

  logging.info("Number of arguments: %s arguments.", str(len(sys.argv)))
  logging.info("Argument List: %s", str(sys.argv))
  time.sleep(20)

if __name__ == "__main__":
  main()
  sys.exit(0)
