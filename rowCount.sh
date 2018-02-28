#!/bin/bash

export HBASE_HOME=/usr/hdp/current/hbase-client
export HADOOP_CLASSPATH=`hadoop classpath`:$HBASE_HOME/lib/*:$HBASE_HOME/conf:./lib

yarn jar rowCount.jar "$@"
