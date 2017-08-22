package kv

import (
	"sync"
	"net/rpc"
	"net"
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

	val, exists := this.data[key]

	if exists {
		rep.Ack = OK
		rep.Val = val
	} else {
		rep.Ack = NE
	}

	return nil
}

func (this *KV) PUT(arg PutArgs, rep *PutAck) error {
	key, val := arg.Key, arg.Val

	this.data[key] = val

	rep.Ack = OK

	return nil
}

type KV_Server struct {
	KV
	lsn net.Listener
}

func (this *KV_Server)Close(_ struct{}, _ *struct{}) error  {
	if this.lsn != nil {
		this.lsn.Close()
	}
	fmt.Println("-----CLOSE-----")
	return  nil
}

func (this *KV_Server) Start()  {
	this.data = make(map[string]string)
	rpc.Register(this)
	lsn, err := net.Listen("unix", "/tmp/kv.socket")
	if err != nil {
		fmt.Println(err)
		return
	}
	this.lsn = lsn
	rpc.Accept(lsn)
}

type KV_Client struct {
	conn Conn
}

func (this *KV_Client)Put(key string, val string) error {
    putArg := PutArgs{key, val}
    putAck := &PutAck{}
    err := this.Call("KV_Server", PutArgs{key, val}, putAck)
    if err != nil {
        return err
    }

    if putAck.Ack != OK {
        return fmt.Errorf()
    }

    return nil
}

func (this *KV_Client)Get(key string)(string, error) {
    getArg := GetArgs{key}
    getAck := &GetAck{}

    err := this.Call("KV_Server", getArg, getAck)




}

func NewKVClient(){
    kvc := new(KV_Client)
    kvc.conn = net.Dial("unix", "/tmp/kv.socket")
    return kvc
}
