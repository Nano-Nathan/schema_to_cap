#!/bin/bash
dir=`dirname "$0"`
"${JAVA_HOME}/bin/java" -classpath "$dir/rtt/lib" -jar "$dir/rtt/lib/rtt.jar" $*
