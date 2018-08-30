#!/bin/sh

. ./COMMON.sh

for i in $SERVER1_EXT $SERVER2_EXT $SERVER3_EXT $SERVER4_EXT; do
    echo Stopping all Wallaroo procs on $i
    ssh -n $USER@$i "killall -9 market-spread sender receiver beam.smp python ; mkdir -p /tmp/run-dir"
done
