package raft

import "log"
import "fmt"

// Debugging
const Debug = 0

func DPrintln(a ...interface{})  {
	if Debug > 0 {
		fmt.Println(a...)
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
