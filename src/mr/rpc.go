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

type TaskType int

const (
	Map    TaskType = 1
	Reduce TaskType = 2
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskAskArgs struct {
	workerPid int
}

type TaskAskReply struct {
	TaskType  TaskType
	taskNo    int
	taskFiles []string
}

type TaskFinishArgs struct {
	workerPid int
	isSucceed bool
}

type TaskFinishReply struct {
	workerPid int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
