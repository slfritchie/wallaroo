#!/bin/sh

. ./COMMON.sh

VALIDATION_TOTAL=2000
VALIDATION_MIDWAY=500

export START_RECEIVER_CMD='env PYTHONPATH="./testing/tools/integration" \
  python ./utils/resilience-demo/validation-receiver.py ${SERVER1}:5555 \
  '$VALIDATION_TOTAL' 11 600 ./received.txt'

export WALLAROO_BIN="./testing/correctness/apps/multi_partition_detector/multi_partition_detector"

export START_SENDER_CMD1='env PYTHONPATH="./testing/tools/integration" \
  python ./utils/resilience-demo/validation-sender.py ${SERVER1}:7000 \
  0 '$VALIDATION_MIDWAY' 100 0.05 11'
export START_SENDER_CMD2='env PYTHONPATH="./testing/tools/integration" \
  python ./utils/resilience-demo/validation-sender.py ${SERVER1}:7000 \
  '$VALIDATION_MIDWAY' '$VALIDATION_TOTAL' 100 0.05 11'

./20-start-2worker-cluster.sh
if [ $? -ne 0 ]; then
    echo STOP with non-zero status
fi

env START_SENDER_CMD="$START_SENDER_CMD1" START_SENDER_BG=n \
    ./30-start-sender.sh
./40-kill-worker.sh 2

####./50-copy-worker-resilience.sh 2 2 3
####./60-restart-worker.sh 2 3

env TARGET_EXT=$SERVER2_EXT ./KLUDGE-TCP-FILES.sh
ssh -n $USER@$SERVER2_EXT "mkdir -p /tmp/run-dir/OLD ; mv -v /tmp/run-dir/m* /tmp/run-dir/OLD"

./60-restart-worker.sh 2 2

env START_SENDER_CMD="$START_SENDER_CMD2" START_SENDER_BG=n \
  ./30-start-sender.sh
S=5; echo SLEEP $S; sleep $S;

echo To stop everything, run: env WALLAROO_BIN="./testing/correctness/apps/multi_partition_detector/multi_partition_detector" ./99-stop-everything.sh
