#!/bin/bash
here=$(dirname "$0")
[[ "$here" = /* ]] || here="$PWD/$here"
#export GOPATH="$here/../../"

echo ""
echo "==> Part I"
(cd "$here/../mapreduce" && go test -run Sequential)
echo ""
echo "==> Part II"
(cd "$here" && sh ./test-wc.sh > /dev/null)
echo ""
echo "==> Part III"
(cd "$here/../mapreduce" && go test -run TestBasic)
echo ""
echo "==> Part IV"
(cd "$here/../mapreduce" && go test -run Failure)
echo ""
echo "==> Part V (challenge)"
(cd "$here" && sh ./test-ii.sh > /dev/null)

rm "$here"/mrtmp.* "$here"/diff.out
