#! /bin/bash

source /tmp/integration-env.sh

echo "Primary= $PRIMARY_HOST"

export LD_LIBRARY_PATH

# Print version information
MV=`mongod --version 2>/dev/null | head -n 1`

echo -n "INFO: Installed MongoDB ($MV): "
echo "$MV" | sed -e 's/.* v//'

ps axx | grep 'mongod$' | awk '{ printf("%s\n", $1); }'

MONGOD_PID=`ps -o pid,comm -u $USER | grep 'mongod$' | awk '{ printf("%s\n", $1); }'`

if [ "x$MONGOD_PID" = "x" ]; then
    echo "ERROR: MongoDB process not found" > /dev/stderr
    exit 1
fi

# JVM/SBT setup
TEST_OPTS="exclude not_mongo26,unit"
SBT_ARGS="-Dtest.primaryHost=$PRIMARY_HOST"
SBT_ARGS="$SBT_ARGS -Dtest.slowPrimaryHost=$PRIMARY_SLOW_PROXY -Dtest.slowProxyDelay=300 -Dtest.slowFailoverRetries=12"

if [ "$MONGO_VER" = "3" ]; then
    TEST_OPTS="exclude mongo2,unit"
fi

if [ "$MONGO_PROFILE" = "ssl" ]; then
    SBT_ARGS="$SBT_ARGS -Dtest.enableSSL=true"
fi

if [ "$MONGO_PROFILE" = "rs" ]; then
    SBT_ARGS="$SBT_ARGS -Dtest.replicaSet=true"
fi

source "$SCRIPT_DIR/jvmopts.sh"

cat > /dev/stdout <<EOF
- JVM options: $JVM_OPTS
- SBT options: $SBT_ARGS
- Test options: $TEST_OPTS
EOF

sbt ++$TRAVIS_SCALA_VERSION 'test:compile' && sleep 30s

export JVM_OPTS

TEST_ARGS=";startMetrics ;project ReactiveMongo ;testOnly -- $TEST_OPTS"
TEST_ARGS="$TEST_ARGS ;project ReactiveMongo-Iteratees ;test-only IterateeSpec"
#TEST_ARGS="$TEST_ARGS ;project ReactiveMongo-Iteratees ;testOnly -- $TEST_OPTS"
#TEST_ARGS="$TEST_ARGS ;project ReactiveMongo-JMX ;testOnly -- $TEST_OPTS"
TEST_ARGS="$TEST_ARGS ;stopMetrics"

sed -e 's/"-deprecation", //' < project/ReactiveMongo.scala > .tmp && mv .tmp project/ReactiveMongo.scala

sbt ++$TRAVIS_SCALA_VERSION $SBT_ARGS "$TEST_ARGS" || (
    PID=`ps -o pid,comm -u $USER | grep 'mongod$' | awk '{ printf("%s\n", $1); }'`

    if [ ! "x$PID" = "x" ]; then
        pid -p $PID
    else
        echo "ERROR: MongoDB process not found" > /dev/stderr
    fi

    tail -n 100 /tmp/mongod.log

    false
)
