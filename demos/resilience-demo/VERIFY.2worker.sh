#!/bin/sh

VALIDATION_TOTAL=2000
VALIDATION_MIDWAY=1000

export START_RECEIVER_CMD='env PYTHONPATH="./testing/tools/integration" \
  python ./demos/resilience-demo/validation-receiver.py ${SERVER1}:5555 \
  '$VALIDATION_TOTAL' 11 600 ./received.txt'

export WALLAROO_BIN="./testing/correctness/apps/multi_partition_detector/multi_partition_detector"

export START_SENDER_CMD2='env PYTHONPATH="./testing/tools/integration" \
  python ./demos/resilience-demo/validation-sender.py ${SERVER1}:7000 \
  '$VALIDATION_MIDWAY' '$VALIDATION_TOTAL' 100 0.05 11'

. ./COMMON.sh

echo To stop everything, run: env WALLAROO_BIN="./testing/correctness/apps/multi_partition_detector/multi_partition_detector" ./99-stop-everything.sh

echo Start 2 worker cluster
./20-start-2worker-cluster.sh
if [ $? -ne 0 ]; then
    echo STOP with non-zero status
fi

echo Send 1st half of messages
. ./SEND-1k.sh
echo Sleep 4 before restarting worker2; sleep 4

echo Kill worker2
./40-kill-worker.sh 2 2

echo Move worker2 journal files, restart worker2 on server 3
./50-copy-worker-resilience.sh 2 2 3
./60-restart-worker.sh 2 3

echo Send 2nd half of messages
env START_SENDER_CMD="$START_SENDER_CMD2" START_SENDER_BG=n \
  ./30-start-sender.sh
ssh -n $USER@$SERVER1_EXT "cd wallaroo ; ./demos/resilience-demo/received-wait-for.sh ./received.txt 11 2000 20"
if [ $? -ne 0 ]; then echo status check failed; exit 7; fi;

echo Run validator to check for sequence validity.
ssh -n $USER@$SERVER1_EXT "cd wallaroo; ./testing/correctness/apps/multi_partition_detector/validator/validator -e 2000  -i -k key_0,key_1,key_2,key_3,key_4,key_5,key_6,key_7,key_8,key_9,key_10"
STATUS=$?

echo Validation status was: $STATUS
exit $STATUS
