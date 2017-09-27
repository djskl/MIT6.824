#!/bin/bash
PIDS=""

for((i=1;i<100;i++))
do
    go test -run 2B > /tmp/raft/$i.txt &
    PIDS=$PIDS" "$!
done

wait $PIDS
