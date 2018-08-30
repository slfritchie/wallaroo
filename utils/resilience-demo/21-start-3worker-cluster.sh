#!/bin/sh

. ./COMMON.sh

./99-stop-everything.sh
ssh -n $USER@$SERVER1_EXT "rm -f /tmp/market-spread* /tmp/run-dir/*"
ssh -n $USER@$SERVER2_EXT "rm -f /tmp/market-spread* /tmp/run-dir/*"
ssh -n $USER@$SERVER3_EXT "rm -f /tmp/market-spread* /tmp/run-dir/*"
ssh -n $USER@$SERVER4_EXT "rm -f /tmp/market-spread* /tmp/run-dir/*"

. ./START-DOS-SERVER.sh

echo Start MUI
ssh -n $USER@$SERVER1_EXT "/home/ubuntu/wallaroo-tutorial/wallaroo-0.5.2/bin/metrics_ui/AppRun start" &
sleep 1

echo Start receiver
ssh -n $USER@$SERVER1_EXT "cd wallaroo ; ./giles/receiver/receiver --ponythreads=1 --ponynoblock --ponypinasio -w -l ${SERVER1}:5555 > /tmp/run-dir/receiver.out 2>&1" &
sleep 2

echo Start initializer
ssh -n $USER@$SERVER1_EXT "cd wallaroo ; ./testing/performance/apps/market-spread/market-spread -i ${SERVER1}:7000,${SERVER1}:7001 -o ${SERVER1}:5555 -m ${SERVER1}:5001 -c ${SERVER1}:12500 -d ${SERVER1}:12501 -t -e ${SERVER1}:5050 -w 3 $W_DOS_SERVER_ARG --ponynoblock > /tmp/run-dir/market-spread1.out 2>&1" &
sleep 2

echo Start worker2
ssh -n $USER@$SERVER2_EXT "cd wallaroo ; ./testing/performance/apps/market-spread/market-spread -i ${SERVER1}:7000,${SERVER1}:7001 -o ${SERVER1}:5555 -m ${SERVER1}:5001 -c ${SERVER1}:12500 -n worker2 --my-control ${SERVER2}:13131 --my-data ${SERVER2}:13132 $W_DOS_SERVER_ARG --ponynoblock > /tmp/run-dir/market-spread2.out 2>&1" &
sleep 2

echo Start worker3
ssh -n $USER@$SERVER3_EXT "cd wallaroo ; ./testing/performance/apps/market-spread/market-spread -i ${SERVER1}:7000,${SERVER1}:7001 -o ${SERVER1}:5555 -m ${SERVER1}:5001 -c ${SERVER1}:12500 -n worker3 --my-control ${SERVER3}:13131 --my-data ${SERVER3}:13132 $W_DOS_SERVER_ARG --ponynoblock > /tmp/run-dir/market-spread3.out 2>&1" &
sleep 2

for i in $SERVER1_EXT $SERVER2_EXT $SERVER3_EXT; do
    echo Check Wallaroo worker on $i
    ssh -n $USER@$i "grep III /tmp/run-dir/mar*out"
done
