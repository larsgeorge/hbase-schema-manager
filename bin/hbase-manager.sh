#!/bin/bash

# set main class
MAIN_CLASS="com.larsgeorge.hadoop.hbase.HBaseManager"

# determine where we are
TOOL_HOME=$(dirname "$0")
TOOL_HOME=$(cd "$TOOL_HOME"; pwd)

# set up classpath
for f in $TOOL_HOME/../*.jar; do
  CLASSPATH=${CLASSPATH}:$f
done
for f in $TOOL_HOME/../lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f
done
echo "using classpath: $CLASSPATH"

java -cp $CLASSPATH $MAIN_CLASS $*