#!/bin/bash
PIDS=""

for((i=0;i<10;i++))
do
    go test -run 2C > /tmp/raft/$i.txt &
    PIDS=$PIDS" "$!
done

wait $PIDS
