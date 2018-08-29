#!/bin/sh

. ./COMMON.sh

echo
echo "NOTE: Using the rsync method, TODO"
echo

ssh -A -n $USER@$SERVER3_EXT "rsync -raH -v -e 'ssh -o \"StrictHostKeyChecking no\"' ${SERVER2}:/tmp/market-spread\* /tmp"

ssh -n $USER@$SERVER3_EXT "(echo $SERVER3 ; echo 3131) > /tmp/market-spread-worker2.tcp-control ; (echo $SERVER3 ; echo 3132) > /tmp/market-spread-worker2.tcp-data"
