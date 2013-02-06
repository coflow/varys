#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin/varys-config.sh"

if [ -f "${VARYS_CONF_DIR}/varys-env.sh" ]; then
  . "${VARYS_CONF_DIR}/varys-env.sh"
fi

# Find the port number for the master
if [ "$VARYS_MASTER_PORT" = "" ]; then
  VARYS_MASTER_PORT=1606
fi

if [ "$VARYS_MASTER_IP" = "" ]; then
  VARYS_MASTER_IP=`hostname`
fi

echo "Master IP: $VARYS_MASTER_IP"

# Launch the slaves
exec "$bin/slaves.sh" cd "$VARYS_HOME" \; "$bin/start-slave.sh" varys://$VARYS_MASTER_IP:$VARYS_MASTER_PORT
