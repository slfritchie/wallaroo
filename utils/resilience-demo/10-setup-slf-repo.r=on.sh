#!/bin/sh

. ./COMMON.sh

ssh -n $USER@$SERVER1_EXT "git clone $REPO_URL"

if [ "$SKIP_CLEAN" = "" ]; then
    ssh -n $USER@$SERVER1_EXT "cd wallaroo ; make clean"
fi

echo ; echo NOTE resilience=on ; echo

if [ "$SKIP_MAKE" = "" ]; then
    ssh -n $USER@$SERVER1_EXT "cd wallaroo ; make clean ; git checkout $REPO_BRANCH ; make PONYCFLAGS='--verbose=1 -d' resilience=on build-testing-performance-apps-market-spread build-giles-all build-utils-cluster_shutdown"
fi

for i in $SERVER2 $SERVER3 $SERVER4; do
    echo rsync to $i
    ssh -A -n $USER@$SERVER1_EXT "rsync -raH --delete -e 'ssh -o \"StrictHostKeyChecking no\"' ~/wallaroo ${i}:"
done