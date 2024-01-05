package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type WorkerProcessStatus int

const (
	WorkerProcessStatusReady   WorkerProcessStatus = 1
	WorkerProcessStatusRunning WorkerProcessStatus = 2
	WorkerProcessStatusExit    WorkerProcessStatus = 3
)

type WorkerStat struct {
	Pid    int
	Status WorkerProcessStatus
}

type MRTask struct {
	taskNo    int
	filename  []string
	taskType  TaskType
	workerPid int // -1 indicates it has not been assigned.
	done      bool
}

type Coordinator struct {
	workerStat map[int]WorkerStat
	workerLock sync.Mutex
	taskStat   []*MRTask
	waitT      chan *MRTask
	finT       chan *MRTask
	nMap       int
	nReduce    int
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
	workerPid := args.WorkerPid

	if len(c.waitT) == 0 {
		reply.TaskType = TaskTypeNon
		return nil
	}

	task := <-c.waitT
	task.workerPid = workerPid

	stat := c.workerStat[workerPid]
	stat.Status = WorkerProcessStatusRunning

	go func() {
		task := task
		stat := stat
		// Sleep 10s
		time.Sleep(10 * 1000 * 1000 * 1000)
		// Reassign tasks.
		if !task.done && stat.Status == WorkerProcessStatusRunning {
			log.Printf("It cost too long to wait task %+v to finish, worker %v may be killed or hang, reassign the task.", task, task.workerPid)
			task.workerPid = -1
			c.waitT <- task
		}
	}()

	reply.TaskType = task.taskType
	reply.Inputs = task.filename
	reply.NReduce = c.nReduce
	reply.TaskNo = task.taskNo
	return nil
}

func (c *Coordinator) TaskFinish(args *TaskFinishArgs, reply *TaskFinishReply) error {
	stat := c.workerStat[args.WorkerPid]
	stat.Status = WorkerProcessStatusReady
	taskNo := args.TaskNo
	if args.TaskType == TaskTypeReduce {
		taskNo += c.nMap
		// Clean up intermediate files
		for _, v := range c.taskStat[taskNo].filename {
			if err := os.Remove(v); err != nil {
				log.Fatal(err)
			}
		}
	}
	task := c.taskStat[taskNo]
	c.finT <- task
	task.done = true
	if len(c.finT) == c.nMap {
		c.initReduceTasks()
	}
	return nil
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	if c.Done() {
		log.Println("Task is done, new workers will not be accepted.")
		return nil
	}
	c.workerLock.Lock()
	c.workerStat[args.WorkerPid] = WorkerStat{
		Pid:    args.WorkerPid,
		Status: WorkerProcessStatusReady,
	}
	c.workerLock.Unlock()
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
	return len(c.finT) == c.nReduce+c.nMap
}

func (c *Coordinator) initReduceTasks() {
	for i := 0; i < c.nReduce; i++ {
		filenames := []string{}
		for j := 0; j < c.nMap; j++ {
			filenames = append(filenames, fmt.Sprintf("mr-%v-%v", j, i))
		}
		task := &MRTask{
			taskNo:    i,
			taskType:  TaskTypeReduce,
			filename:  filenames,
			workerPid: -1,
			done:      false,
		}
		c.taskStat = append(c.taskStat, task)
		c.waitT <- task
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)
	c := Coordinator{
		workerStat: map[int]WorkerStat{},
		taskStat:   []*MRTask{},
		waitT:      make(chan *MRTask, nMap+nReduce),
		finT:       make(chan *MRTask, nMap+nReduce),
		nMap:       nMap,
		nReduce:    nReduce,
	}
	for i, filename := range files {
		task := &MRTask{
			taskNo:    i,
			filename:  []string{filename},
			taskType:  TaskTypeMap,
			workerPid: -1,
			done:      false,
		}
		c.taskStat = append(c.taskStat, task)
		c.waitT <- task
	}
	c.server()
	return &c
}
