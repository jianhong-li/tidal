#!/bin/sh

#===========================================================================================
# Java Environment Setting
#===========================================================================================

if [ ! -f "$JAVA_HOME/bin/java" ];then
  echo "Please set the JAVA_HOME variable in your environment, We need java(x64)!"
  exit 1
fi

export JAVA=$JAVA_HOME/bin/java
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=.:${BASE_DIR}/conf:${CLASSPATH}

#===========================================================================================
# JVM Configuration
#===========================================================================================

JAVA_OPT="${JAVA_OPT} -server -Xmx1024m -Xms128m"
JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC"
JAVA_OPT="${JAVA_OPT} -Djava.ext.dirs=${BASE_DIR}/lib"
JAVA_OPT="${JAVA_OPT} -cp ${CLASSPATH}"

$JAVA ${JAVA_OPT} $@
