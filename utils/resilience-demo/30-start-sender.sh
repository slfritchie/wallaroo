#!/bin/sh

. ./COMMON.sh

if [ "$SEND_INITIAL_NBBO" = y ]; then
    echo Run NBBO initial sender
    ssh -n $USER@$SERVER1_EXT "cd wallaroo ; ./giles/sender/sender -h ${SERVER1}:$NBBO_PORT -m 1400 -s 10 -i 50_000_000 -f ./testing/data/market_spread/nbbo/1400-symbols_initial-nbbo-fixish.msg -r --ponythreads=1 -y -g 57 > /tmp/run-dir/sender.initial.`date +%s`.out 2>&1" > /dev/null 2>&1
fi

if [ "$SEND_ORDERS" = y ]; then
    echo Start Orders sender
    ssh -n $USER@$SERVER1_EXT "cd wallaroo ; ./giles/sender/sender -h ${SERVER1}:$ORDERS_PORT -m 999000000 -s 10 -i 50_000_000 -f ./testing/data/market_spread/orders/350-symbols_orders-fixish.msg -r --ponythreads=1 -y -g 57 > /tmp/run-dir/sender.out 2>&1" > /dev/null 2>&1 &
fi

if [ "$SEND_NBBO" = y ]; then
    echo Start NBBO sender
    ssh -n $USER@$SERVER1_EXT "cd wallaroo ; ./giles/sender/sender -h ${SERVER1}:$NBBO_PORT -m 999000000 -s 10 -i 50_000_000 -f ./testing/data/market_spread/nbbo/350-symbols_nbbo-fixish.msg -r --ponythreads=1 -y -g 57 > /tmp/run-dir/sender.out 2>&1" > /dev/null 2>&1 &
fi
