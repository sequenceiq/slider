#!/bin/bash


java -jar target/hoya-0.3-SNAPSHOT.jar org.apache.hadoop.hoya.yarn.client.HoyaClient "$@"
