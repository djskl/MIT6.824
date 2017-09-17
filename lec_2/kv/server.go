package kv

import (
	"net/rpc"
	"net"
	"fmt"
)

type KV_Server struct {
	KV
	lsn net.Listener
}

func (this *KV_Server)Close(_ struct{}, _ *struct{}) error  {
	if this.lsn != nil {
		this.lsn.Close()
	}
	fmt.Println("-----EXIST-----")
	return  nil
}

func (this *KV_Server) Start()  {
	rpc.Register(this)
	fmt.Println("-----START-----")
	lsn, err := net.Listen("unix", "/tmp/kv.socket")
	if err != nil {
		fmt.Println(err)
		return
	}
	this.lsn = lsn
	rpc.Accept(lsn)
}

func NewKVServer() *KV_Server {
	kvs := new(KV_Server)
	kvs.data = make(map[string]string)
	return kvs
}

