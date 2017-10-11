package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
	job int
	available bool
	jobType JobType
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	processJob(mr, Map, mr.nMap, mr.nReduce)
	processJob(mr, Reduce, mr.nReduce, mr.nMap)
	return mr.KillWorkers()
}

func processJob(mr *MapReduce,jobType JobType, jobs int, otherJobs int, ){

	for i := 0; i < jobs; i++ {
		go func(jobNum int) {
			done := false
			var worker string
			for !done {
				worker = <-mr.registerChannel
				done = call(worker, "Worker.DoJob",
					DoJobArgs{mr.file, jobType, jobNum, otherJobs}, new(DoJobReply))
			}
			mr.activeWorkers <- worker // channel of active workers

			// worker registration
			workerInfo := new(WorkerInfo)
			workerInfo.job = jobNum
			workerInfo.available = false
			workerInfo.jobType = jobType
			mr.Workers[worker] = workerInfo

			mr.registerChannel <- worker // making worker available for reuse
		}(i)
	}

	//All maps have to finish before reduce can begin
	for j := 0; j < jobs; j++ {

		worker:= <-mr.activeWorkers
		val, ok := mr.Workers[worker]
		if ok{
			val.available = true
			val.jobType = ""
			val.job = -1
		}
	}
}
