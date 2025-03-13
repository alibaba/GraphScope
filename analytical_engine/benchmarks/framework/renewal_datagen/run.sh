#!/bin/bash

if [ ! -f params.ini ]; then
  echo "Parameters file (params.ini) not found."
  exit 1
fi

# DEFAULT_HADOOP_HOME=/home/user/hadoop-3.2.1 #change to your hadoop folder
DEFAULT_HADOOP_HOME=/usr/local/hadoop-running
DEFAULT_LDBC_SNB_DATAGEN_HOME=`pwd` #change to your ldbc_snb_datagen folder

# allow overriding configuration from outside via environment variables
# i.e. you can do
#     HADOOP_HOME=/foo/bar LDBC_SNB_DATAGEN_HOME=/baz/quux ./run.sh
# instead of changing the contents of this file
HADOOP_HOME=${HADOOP_HOME:-$DEFAULT_HADOOP_HOME}
LDBC_SNB_DATAGEN_HOME=${LDBC_SNB_DATAGEN_HOME:-$DEFAULT_LDBC_SNB_DATAGEN_HOME}

export HADOOP_HOME
export LDBC_SNB_DATAGEN_HOME

echo ===============================================================================
echo Running generator with the following parameters:
echo -------------------------------------------------------------------------------
echo LDBC_SNB_DATAGEN_HOME: $LDBC_SNB_DATAGEN_HOME
echo JAVA_HOME: $JAVA_HOME
echo HADOOP_HOME: $HADOOP_HOME
echo HADOOP_CLIENT_OPTS: $HADOOP_CLIENT_OPTS
echo ===============================================================================

mvn clean
mvn -DskipTests assembly:assembly

if [ "$(uname)" == "Darwin" ]; then
  zip -d $LDBC_SNB_DATAGEN_HOME/target/ldbc_snb_datagen-1.0.0-jar-with-dependencies.jar META-INF/LICENSE
fi

$HADOOP_HOME/bin/hadoop jar $LDBC_SNB_DATAGEN_HOME/target/ldbc_snb_datagen-1.0.0-jar-with-dependencies.jar $LDBC_SNB_DATAGEN_HOME/params.ini

rm -f m*personFactors*
rm -f .m*personFactors*
rm -f m*activityFactors*
rm -f .m*activityFactors*
rm -f m0friendList*
rm -f .m0friendList*
