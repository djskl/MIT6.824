#!/bin/bash
PIDS=""

for((i=0;i<50;i++))
do
    go test -run TestPersist12C > /tmp/raft/$i.txt &
    PIDS=$PIDS" "$!
done

wait $PIDS
