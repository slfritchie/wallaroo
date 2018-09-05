#!/bin/sh

. ./COMMON.sh

if [ -z "$1" -o -z "$2" ]; then
    echo "usage: $0 source-worker-number target-worker-number ... where worker-number = 2-4"
    exit 1
else
    SOURCE_WORKER=$1
    eval 'TARGET=$SERVER'$2
    eval 'TARGET_EXT=$SERVER'$2'_EXT'
fi

ssh -n $USER@$TARGET_EXT "cd wallaroo ; $WALLAROO_BIN -i ${SERVER1}:7000,${SERVER1}:7001 -o ${SERVER1}:5555 -m ${SERVER1}:5001 -c ${SERVER1}:12500 -n worker${SOURCE_WORKER} --my-control ${TARGET}:3131 --my-data ${TARGET}:3132 --ponynoblock --resilience-disable-io-journal > /tmp/run-dir/${WALLAROO_NAME}${SOURCE_WORKER}.`date +%s`.out 2>&1" > /dev/null 2>&1 &
if [ -z "$RESTART_SLEEP" ]; then
    sleep 2
else
    echo sleeping for RESTART_SLEEP=$RESTART_SLEEP
    sleep $RESTART_SLEEP
fi

for i in $SERVER1_EXT $TARGET_EXT; do
    echo Check Wallaroo worker on $i
    ssh -n $USER@$i "grep III /tmp/run-dir/mar*out"
done
