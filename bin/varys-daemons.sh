#!/usr/bin/env bash

# Run a Varys command on all slave hosts.

usage="Usage: varys-daemons.sh [--config confdir] [--hosts hostlistfile] [start|stop] command args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin/varys-config.sh"

exec "$bin/slaves.sh" cd "$VARYS_HOME" \; "$bin/varys-daemon.sh" "$@"
