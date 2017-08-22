package main

import "MIT6.824/lec_2/kv"

func main() {
	kvs := kv.NewKVServer()
	kvs.Start()
}
