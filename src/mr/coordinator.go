package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type workerProcessStatus int

const (
	workerProcessStatusReady   workerProcessStatus = 1
	workerProcessStatusRunning workerProcessStatus = 2
	workerProcessStatusExit    workerProcessStatus = 3
)

type workerProfile struct {
	status   workerProcessStatus
	filename string
}

type Coordinator struct {
	WorkerStatus map[int]workerProcessStatus // pid of workers
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) TaskAsk(args *TaskAskArgs, reply *TaskAskReply) error {
	log.Printf("Hello, worker %v.", args.WorkerPid)
	reply.TaskType = TaskTypeExit
	c.WorkerStatus[args.WorkerPid] = workerProcessStatusExit
	return nil
}

func (c *Coordinator) TaskFinish(args *TaskFinishArgs, reply *TaskFinishReply) error {
	// TODO
	return nil
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.WorkerStatus[args.WorkerPid] = workerProcessStatusReady
	reply.CoordinatorPid = os.Getpid()
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
	log.Println("Coordinator starts...")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if (len(c.WorkerStatus)) == 0 {
		return false
	}
	for _, v := range c.WorkerStatus {
		if v != workerProcessStatusExit {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		map[int]workerProcessStatus{},
	}

	// Your code here.

	c.server()
	return &c
}
