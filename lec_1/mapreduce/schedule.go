package mapreduce

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"log"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	var inputArgs chan string
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
		inputArgs = make(chan string, ntasks)
		for idx, mf := range (mapFiles) {
			arg := strconv.Itoa(idx) + " " + mf
			inputArgs <- arg
		}
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		inputArgs = make(chan string, ntasks)
		for idx := 0; idx < ntasks; idx++ {
			arg := strconv.Itoa(idx)
			inputArgs <- arg
		}
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	var wg sync.WaitGroup
	wg.Add(ntasks)

	go func() {
		for {

			wk := <-registerChan

			go func() {
				input_arg := <-inputArgs
				args := strings.Fields(input_arg)
				taskNum, err := strconv.Atoi(args[0])
				if err != nil {
					log.Fatalf("Number(%s) parse error: %s\n", args[0], err)
				}

				taskArg := &DoTaskArgs{
					JobName:       jobName,
					Phase:         phase,
					TaskNumber:    taskNum,
					NumOtherPhase: n_other,
				}

				if phase == mapPhase {
					taskArg.File = args[1]
				}

				ok := call(wk, "Worker.DoTask", &taskArg, new(struct{}))

				defer func() {
					registerChan <- wk
				}()

				if !ok {
					inputArgs <- input_arg
				} else {
					wg.Done()
				}

			}()
		}
	}()

	wg.Wait()

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
