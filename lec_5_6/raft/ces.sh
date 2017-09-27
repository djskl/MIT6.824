#!/bin/bash
PIDS=""

for((i=1;i<100;i++))
do
    go test -run 2A > /tmp/raft/$i.txt &
    PIDS=$PIDS" "$!
done

wait $PIDS
