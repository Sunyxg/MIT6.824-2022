package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	StartTime time.Time
	State     int // 0:wite  1:running 2:finish
}

type Coordinator struct {
	// Your definitions here.
	NumReduce    int
	MapRemain    int
	ReduceRemain int
	MapTask      []Task
	ReduceTask   []Task
	Files        []string
	mu           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// // an example RPC handler.
// //
// // the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

func (c *Coordinator) Heartbeat(req *GetRequest, reps *HeartbeatResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	currTime := time.Now()
	// Map任务尚未完全完成
	if c.MapRemain > 0 {
		for i, task := range c.MapTask {
			dur := currTime.Sub(task.StartTime)
			if task.State == 0 || (task.State == 1 && dur.Seconds() > 10.0) {
				c.MapTask[i].State = 1
				c.MapTask[i].StartTime = currTime
				reps.TaskId = i
				reps.FileName = c.Files[i]
				reps.NReduce = c.NumReduce
				reps.Jobtype = 0
				return nil
			}
		}
		reps.Jobtype = 1
		return nil
	}

	// 进入Reduce工作阶段
	if c.ReduceRemain > 0 {
		for i, task := range c.ReduceTask {
			dur := currTime.Sub(task.StartTime)
			if task.State == 0 || (task.State == 1 && dur.Seconds() > 10.0) {
				c.ReduceTask[i].State = 1
				c.ReduceTask[i].StartTime = currTime
				reps.TaskId = i
				reps.NMap = len(c.MapTask)
				reps.Jobtype = 2
				return nil
			}
		}
		// 等待已分配的Reduce任务完成
		reps.Jobtype = 1
	} else {
		// Reduce任务已经全部完成，避免worker提前退出
		reps.Jobtype = 3
	}

	return nil
}

func (c *Coordinator) Taskover(req *TaskDoneRequest, reps *TaskDoneResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := req.Taskid
	MoR := req.MorR
	if id >= 0 {
		if MoR == 0 {
			if c.MapTask[id].State != 2 {
				c.MapTask[id].State = 2
				c.MapRemain = c.MapRemain - 1
				// fmt.Printf("get mapid %v, remainmap %v\n", id, c.MapRemain)

			}
		} else if MoR == 1 {
			if c.ReduceTask[id].State != 2 {
				c.ReduceTask[id].State = 2
				c.ReduceRemain = c.ReduceRemain - 1
				// fmt.Printf("get Reduceid %v, remainReduce %v\n", id, c.ReduceRemain)
			}
		}

	}
	reps.Done = true

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := false
	if c.ReduceRemain == 0 && c.MapRemain == 0 {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.NumReduce = nReduce
	c.MapRemain = len(files)
	c.ReduceRemain = nReduce
	for i := 0; i < nReduce; i++ {
		c.ReduceTask = append(c.ReduceTask, Task{State: 0})
	}
	for _, filename := range files {
		c.Files = append(c.Files, filename)
		c.MapTask = append(c.MapTask, Task{State: 0})
	}

	c.server()
	return &c
}
