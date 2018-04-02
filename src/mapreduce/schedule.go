package mapreduce

import (
    "fmt"
    "sync"
)
//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
    //tasksChannel := make(chan int, ntasks)
    //unfinishedNum := ntasks
    c := make(chan string)
    quit := make(chan bool)
    var wg sync.WaitGroup
    go func() {
        for {
            select {
            case s := <-registerChan:  go func(){c<-s}()
            case <-quit: return
            }
        }
    }()
    for i := 0; i < ntasks; i++ {
        wg.Add(1)
        go func(taskNumber int) {
            defer wg.Done()
            ok := false
            for ok == false {
                address := <-c
                args := new(DoTaskArgs)
                args.JobName = jobName
                args.Phase = phase
                args.NumOtherPhase = n_other
                args.TaskNumber = taskNumber
                if phase == mapPhase {
                    args.File = mapFiles[taskNumber]
                }
                ok = call(address, "Worker.DoTask", args, new(struct{}))
                if ok == true {
                    go func() {c <- address}()
                }
            }
        }(i)
    }
    wg.Wait()
    quit <- true
	fmt.Printf("Schedule: %v done\n", phase)
}
func sched(address string,jobName string, wg sync.WaitGroup) {
}
