package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.
type GetRequest struct {
}

type TaskDoneRequest struct {
	MorR   int // 0:Map 1:Reduce
	Taskid int
}

type TaskDoneResponse struct {
	Done bool
}

type HeartbeatResponse struct {
	Jobtype  int
	FileName string
	TaskId   int
	NReduce  int
	NMap     int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
