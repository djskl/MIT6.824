package kv

import (
	"sync"
	"fmt"
)

const (
	OK = "OK"
	NE = "Not Exist"
)

type PutArgs struct {
	Key string
	Val string
}

type PutAck struct {
	Ack string
}

type GetArgs struct {
	Key string
}

type GetAck struct {
	Ack string
	Val string
}

type KV struct {
	sync.Mutex
	data map[string]string
}

func (this *KV) GET(arg GetArgs, rep *GetAck) error {
	key := arg.Key

	this.Lock()
	defer this.Unlock()

	val, exists := this.data[key]

	if exists {
		rep.Ack = OK
		rep.Val = val
	} else {
		rep.Ack = NE
	}

	fmt.Println("GET", key, val)

	return nil
}

func (this *KV) PUT(arg PutArgs, rep *PutAck) error {
	key, val := arg.Key, arg.Val

	this.Lock()
	defer this.Unlock()

	this.data[key] = val
	rep.Ack = OK

	fmt.Println("SET", key, val)

	return nil
}