#!/bin/bash
PIDS=""

for((i=0;i<3;i++))
do
    go test -run TestFigure8Unreliable2C > /tmp/raft/$i.txt &
    PIDS=$PIDS" "$!
done

wait $PIDS
