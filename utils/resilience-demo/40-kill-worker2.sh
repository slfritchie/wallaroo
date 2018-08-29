#!/bin/sh

. ./COMMON.sh

ssh -n $USER@$SERVER2_EXT "killall -9v market-spread"
