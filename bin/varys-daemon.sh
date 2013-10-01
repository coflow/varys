#!/usr/bin/env bash

# This Varys framework script is a modified version of the Apache Hadoop framework
# script, available under the Apache 2 license:
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

# Runs a Varys command as a daemon.
#
# Environment Variables
#
#   VARYS_CONF_DIR  Alternate conf dir. Default is ${VARYS_PREFIX}/conf.
#   VARYS_LOG_DIR   Where log files are stored.  PWD by default.
#   VARYS_MASTER    host:path where varys code should be rsync'd from
#   VARYS_PID_DIR   The pid files are stored. /tmp by default.
#   VARYS_IDENT_STRING   A string representing this instance of varys. $USER by default
#   VARYS_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: varys-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] (start|stop) <varys-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin/varys-config.sh"

# get arguments
startStop=$1
shift
command=$1
shift

varys_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ -f "${VARYS_CONF_DIR}/varys-env.sh" ]; then
  . "${VARYS_CONF_DIR}/varys-env.sh"
fi

if [ "$VARYS_IDENT_STRING" = "" ]; then
  export VARYS_IDENT_STRING="$USER"
fi

# get log directory
if [ "$VARYS_LOG_DIR" = "" ]; then
  export VARYS_LOG_DIR="$VARYS_HOME/logs"
fi
mkdir -p "$VARYS_LOG_DIR"
touch $VARYS_LOG_DIR/.varys_test > /dev/null 2>&1
TEST_LOG_DIR=$?
if [ "${TEST_LOG_DIR}" = "0" ]; then
  rm -f $VARYS_LOG_DIR/.varys_test
else
  chown $VARYS_IDENT_STRING $VARYS_LOG_DIR 
fi

if [ "$VARYS_PID_DIR" = "" ]; then
  VARYS_PID_DIR=/tmp
fi

# some variables
export VARYS_LOGFILE=varys-$VARYS_IDENT_STRING-$command-$HOSTNAME.log
export VARYS_ROOT_LOGGER="INFO,DRFA"
log=$VARYS_LOG_DIR/varys-$VARYS_IDENT_STRING-$command-$HOSTNAME.out
pid=$VARYS_PID_DIR/varys-$VARYS_IDENT_STRING-$command.pid

# Set default scheduling priority
if [ "$VARYS_NICENESS" = "" ]; then
    export VARYS_NICENESS=0
fi


case $startStop in

  (start)
    
    mkdir -p "$VARYS_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    if [ "$VARYS_MASTER" != "" ]; then
      echo rsync from $VARYS_MASTER
      rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' $VARYS_MASTER/ "$VARYS_HOME"
    fi

    varys_rotate_log $log
    echo starting $command, logging to $log
    cd "$VARYS_PREFIX"
    nohup nice -n $VARYS_NICENESS "$VARYS_PREFIX"/run $command "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $pid`
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


