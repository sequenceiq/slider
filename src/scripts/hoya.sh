#!/bin/bash


java -jar target/hoya-0.0.1-SNAPSHOT.jar org.apache.hadoop.hoya.yarn.client.HoyaClient "$@"
