#!/bin/sh

. ./COMMON.sh

if [ $RESTORE_VIA_JOURNAL_DUMP = y ]; then
    echo Rsync journal file from DOS server $DOS_SERVER to $SERVER3
    ssh -n $USER@$SERVER3_EXT "rm -f /tmp/market-spread*"
    ssh -A -n $USER@$SERVER3_EXT "rsync -raH -v -e 'ssh -o \"StrictHostKeyChecking no\"' ${DOS_SERVER}:/tmp/dos-data/worker2/\* /tmp"

    echo Extract journalled I/O ops from the journal file
    # ssh -n $USER@$SERVER3_EXT "echo BEFORE ; ls -l /tmp/mar*"
    ssh -n $USER@$SERVER3_EXT "cd wallaroo ; python ./utils/journal-dump/journal-dump.py /tmp/market-spread-worker2.journal"
    # ssh -n $USER@$SERVER3_EXT "echo AFTER ; ls -l /tmp/mar*"
    # sleep 3
else
    echo
    echo "NOTE: rsync all resilience files directly from 'failed' worker (cheating)"
    echo

    ssh -A -n $USER@$SERVER3_EXT "rsync -raH -v -e 'ssh -o \"StrictHostKeyChecking no\"' ${SERVER2}:/tmp/market-spread\* /tmp"
fi

echo
echo "NOTE: Kludge to fix up tcp-control and tcp-data files, TODO"
echo

ssh -n $USER@$SERVER3_EXT "(echo $SERVER3 ; echo 3131) > /tmp/market-spread-worker2.tcp-control ; (echo $SERVER3 ; echo 3132) > /tmp/market-spread-worker2.tcp-data"

echo Rename market-spread-worker2.evlog.journal '->' market-spread-worker2.evlog
ssh -n $USER@$SERVER3_EXT "mv /tmp/market-spread-worker2.evlog.journal /tmp/market-spread-worker2.evlog"
