package raft

import "log"
import (
	"fmt"
	"time"
)

const HEARTBEAT_TIMEOUT = 50 * time.Millisecond
const ELECTION_TIMEOUT_BASE = 200
const ELECTION_TIMEOUT_FACT = 300

// Debugging
const Debug = 0

func DPrintln(a ...interface{})  {
	if Debug > 0 {
		fmt.Println(a...)
	}
}

func DebugPrintf(format string, a ...interface{}) (n int, err error)  {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
