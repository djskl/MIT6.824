package main

import (
	"fmt"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(3)

	ch1 := make(chan string, 3)

	ch1 <- "a"
	ch1 <- "b"
	ch1 <- "c"

	go func() {
		for {
			fmt.Println("-----------")

			x := <- ch1

			go func() {
				fmt.Println(x)
				wg.Done()
			}()

		}
	}()

	fmt.Println("xxxxxxxxxxxxx")

	wg.Wait()

	fmt.Println("all is over")

}
