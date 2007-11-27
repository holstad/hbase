#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# included in all the hbase scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*
# Modelled after $HADOOP_HOME/bin/hadoop-env.sh.

# resolve links - $0 may be a softlink

this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# the root of the hbase installation
export HBASE_HOME=`dirname "$this"`/..

#check to see if the conf dir or hadoop home are given as an optional arguments
while [ $# -gt 1 ]
do
  if [ "--config" = "$1" ]
  then
    shift
    confdir=$1
    shift
    HADOOP_CONF_DIR=$confdir
  elif [ "--hbaseconfig" = "$1" ]
  then
    shift
    confdir=$1
    shift
    HBASE_CONF_DIR=$confdir
  elif [ "--hadoop" = "$1" ]
  then
    shift
    home=$1
    shift
    HADOOP_HOME=$home
  elif [ "--hosts" = "$1" ]
  then
    shift
    hosts=$1
    shift
    HBASE_REGIONSERVERS=$hosts
  else
    # Presume we are at end of options and break
    break
  fi
done
 
# If no hadoop home specified, then we assume its above this directory.
# Can be in one of two places.  If we've been packaged, then it'll be
# two levels above us.  If we are running from src at src/contrib/hbase
# or from the build directory at build/contrib/hbase, then its three
# levels up.  Look for the hadoop script.
if [ "$HADOOP_HOME" = "" ]; then
    if [ -f "$HBASE_HOME/../../bin/hadoop" ]; then
      HADOOP_HOME="$HBASE_HOME/../../"
    else
      HADOOP_HOME="$HBASE_HOME/../../../"
    fi
fi

# Allow alternate hadoop conf dir location.
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/conf}"
# Allow alternate hbase conf dir location.
HBASE_CONF_DIR="${HBASE_CONF_DIR:-$HBASE_HOME/conf}"
# List of hbase regions servers.
HBASE_REGIONSERVERS="${HBASE_REGIONSERVERS:-$HBASE_CONF_DIR/regionservers}"
