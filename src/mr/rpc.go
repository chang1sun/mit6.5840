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
	TaskTypeMap    TaskType = 1
	TaskTypeReduce TaskType = 2
	TaskTypeExit   TaskType = 3
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

type RegisterArgs struct {
	WorkerPid int
}

type RegisterReply struct {
	CoordinatorPid int
}

type TaskAskArgs struct {
	WorkerPid int
}

type TaskAskReply struct {
	TaskType TaskType
	TaskNo   int
	NReduce  int
	Inputs   []string
}

type TaskFinishArgs struct {
	WorkerPid int
}

type TaskFinishReply struct {
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
