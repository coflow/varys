#!/usr/bin/env bash

# This file contains environment variables required to run Varys. Copy it as
# varys-env.sh and edit that to configure Varys for your site. At a minimum,
# the following variable should be set (also see KNOWN ISSUES below):
# - SCALA_HOME, to point to your Scala installation
#
# You can set variables for using non-standard IP/ports:
# - VARYS_MASTER_IP, to bind the master to a different IP address
# - VARYS_MASTER_PORT / VARYS_MASTER_WEBUI_PORT, to use non-default ports
# - VARYS_SLAVE_PORT / VARYS_SLAVE_WEBUI_PORT / VARYS_SLAVE_COMM_PORT, to use non-default ports
#
# Finally, Varys also relies on the following variables, but these can be set
# on just the *master*, and will automatically be propagated to slaves:
# - VARYS_CLASSPATH, to add elements to Varys's classpath
# - VARYS_JAVA_OPTS, to add JVM options
# - VARYS_MEM, to change the amount of memory used per node (this should
#   be in the same format as the JVM's -Xmx option, e.g. 300m or 1g).
# - VARYS_LIBRARY_PATH, to add extra search paths for native libraries.
#
# KNOWN ISSUE(S)
# - VARYS_MASTER_IP must be set manually for now
