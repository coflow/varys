#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# Set VARYS_PUBLIC_DNS so slaves can be linked in master web UI
if [ "$VARYS_PUBLIC_DNS" = "" ]; then
    # If we appear to be running on EC2, use the public address by default:
    if [[ `hostname` == *ec2.internal ]]; then
        export VARYS_PUBLIC_DNS=`wget -q -O - http://instance-data.ec2.internal/latest/meta-data/public-hostname`
    fi
fi

"$bin"/varys-daemon.sh start varys.framework.slave.Slave $1
