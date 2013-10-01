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

# Run a shell command on all slave hosts.
#
# Environment Variables
#
#   VARYS_SLAVES    File naming remote hosts.
#     Default is ${VARYS_CONF_DIR}/slaves.
#   VARYS_CONF_DIR  Alternate conf dir. Default is ${VARYS_HOME}/conf.
#   VARYS_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   VARYS_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: slaves.sh [--config confdir] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin/varys-config.sh"

# If the slaves file is specified in the command line,
# then it takes precedence over the definition in 
# varys-env.sh. Save it here.
HOSTLIST=$VARYS_SLAVES

if [ -f "${VARYS_CONF_DIR}/varys-env.sh" ]; then
  . "${VARYS_CONF_DIR}/varys-env.sh"
fi

if [ "$HOSTLIST" = "" ]; then
  if [ "$VARYS_SLAVES" = "" ]; then
    export HOSTLIST="${VARYS_CONF_DIR}/slaves"
  else
    export HOSTLIST="${VARYS_SLAVES}"
  fi
fi

echo $"${@// /\\ }"

# By default disable strict host key checking
if [ "$VARYS_SSH_OPTS" = "" ]; then
  VARYS_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

for slave in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
 ssh $VARYS_SSH_OPTS $slave $"${@// /\\ }" \
   2>&1 | sed "s/^/$slave: /" &
 if [ "$VARYS_SLAVE_SLEEP" != "" ]; then
   sleep $VARYS_SLAVE_SLEEP
 fi
done

wait
