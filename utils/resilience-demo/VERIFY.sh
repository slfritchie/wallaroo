#!/bin/sh

VALIDATION_TOTAL=2000
VALIDATION_MIDWAY=500

export START_RECEIVER_CMD='env PYTHONPATH="./testing/tools/integration" \
  python ./utils/resilience-demo/validation-receiver.py ${SERVER1}:5555 \
  '$VALIDATION_TOTAL' 11 60 ./received.txt'

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

echo pause for 5
sleep 5
echo done

./30-start-sender.sh
wait

./40-kill-worker.sh 2
./50-copy-worker-resilience.sh 2 2 3
./60-restart-worker.sh 2 3

