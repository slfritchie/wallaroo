#!/bin/sh

. ./COMMON.sh

ssh -n $USER@$SERVER1_EXT "cd wallaroo ; ./giles/sender/sender -h ${SERVER1}:7000 -m 999000000 -s 10 -i 50_000_000 -f ./testing/data/market_spread/orders/350-symbols_orders-fixish.msg -r --ponythreads=1 -y -g 57 > /tmp/run-dir/sender.out 2>&1" &

