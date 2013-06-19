#!/usr/bin/env bash

# Starts the master on the machine this script is executed on.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin/varys-config.sh"

"$bin"/varys-daemon.sh stop varys.framework.master.Master
"$bin"/varys-daemon.sh stop varys.framework.slave.Slave