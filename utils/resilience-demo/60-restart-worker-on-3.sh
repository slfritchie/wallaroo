#!/bin/sh

. ./COMMON.sh

ssh -n $USER@$SERVER3_EXT "cd wallaroo ; ./testing/performance/apps/market-spread/market-spread -i ${SERVER1}:7000,${SERVER1}:7001 -o ${SERVER1}:5555 -m ${SERVER1}:5001 -c ${SERVER1}:12500 -n worker2 --my-control ${SERVER3}:3131 --my-data ${SERVER3}:3132 --ponynoblock --resilience-disable-io-journal > /tmp/run-dir/market-spread2b.out 2>&1" &
sleep 2

for i in $SERVER1_EXT $SERVER3_EXT; do
    echo Check Wallaroo worker on $i
    ssh -n $USER@$i "grep III /tmp/run-dir/mar*out"
done
