package kv

import (
	"net/rpc"
	"fmt"
)

type KV_Client struct {
	conn *rpc.Client
}

func (this *KV_Client)Put(key string, val string) error {
    putArg := PutArgs{key, val}
    putAck := new(PutAck)
    err := this.conn.Call("KV_Server.PUT", putArg, putAck)
    if err != nil {
        return err
    }

    if putAck.Ack != OK {
        return fmt.Errorf("put error (%s, %s)", key, val)
    }

    return nil
}

func (this *KV_Client)Get(key string)(string, error) {
    getArg := GetArgs{key}
    getAck := &GetAck{}

    err := this.conn.Call("KV_Server.GET", getArg, getAck)

	if err != nil {
		return "", err
	}

	if getAck.Ack != OK {
		return "", fmt.Errorf("GET ERROR: %s", key)
	}

	return getAck.Val, nil
}

func NewKVClient() (*KV_Client, error) {
    kvc := new(KV_Client)
    conn, err := rpc.Dial("unix", "/tmp/kv.socket")
	if err != nil {
		return nil, fmt.Errorf("connect error")
	}
	kvc.conn = conn
    return kvc, nil
}
