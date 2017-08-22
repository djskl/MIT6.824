package main

import (
	"fmt"
)

func main() {

	x := make(map[string]int)

	x["x"]++

	fmt.Println(x["x"])

}
