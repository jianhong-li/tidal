#!/bin/sh

if [ ! -f "${TIDAL_HOME}/bin/runserver.sh" ];then
  echo "Please set the TIDAL_HOME variable in your environment!"
  exit 1;
fi

sh ${TIDAL_HOME}/bin/runserver.sh tidal.server.ServerStartup $@
