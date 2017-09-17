package main

import (
	"MIT6.824/lec_2/kv"
	"fmt"
	"os"
)

func main() {
	kvc, err := kv.NewKVClient()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	err = kvc.Put("hello", "world")
	if err != nil {
		fmt.Println(err)
	}

	val, err := kvc.Get("hello")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(val)

	val, err = kvc.Get("xxx")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(val)

}
