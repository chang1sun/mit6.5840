package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for true {
		taskType, taskNo, taskFiles, err := AskForTask()
		if err != nil {
			log.Fatalf("Failed to call `AskForTask`. Reason: %w", err)
			return
		}

		switch taskType {
		case Map:
			intermediate := []KeyValue{}
			for _, filename := range taskFiles {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
			}

			sort.Sort(ByKey(intermediate))

			tmpFile, err := os.CreateTemp("../target", "map_tmp")
			if err != nil {
				log.Fatal(err)
			}
			err = json.NewEncoder(tmpFile).Encode(intermediate)
			if err != nil {
				log.Fatal(err)
			}
			os.Rename(tmpFile.Name(), fmt.Sprintf("../target/map_"))

		case Reduce:
			// todo
		default:
			log.Fatalf("Invaild TaskType: %v")
		}

		//
		// handle map/reduce work
		//
		FinishTask()

		// wait 200ms to continue.
		time.Sleep(200 * 1000 * 1000)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

/**
 * Ask for map/reduce tasks.
 */
func AskForTask() (TaskType, int, []string, error) {

	args := TaskAskArgs{
		os.Getpid(),
	}
	reply := TaskAskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	err := call("Coordinator.Example", &args, &reply)
	if err != nil {
		return reply.TaskType, reply.taskNo, reply.taskFiles, nil
	} else {
		return 0, 0, nil, err
	}
}

/**
 * Finishs for map/reduce tasks.
 */
func FinishTask() error {
	// args := TaskAskArgs{
	// 	os.Getpid(),
	// }
	// reply := TaskAskReply{}

	// // send the RPC request, wait for the reply.
	// // the "Coordinator.Example" tells the
	// // receiving server that we'd like to call
	// // the Example() method of struct Coordinator.
	// err := call("Coordinator.Example", &args, &reply)
	// if err != nil {
	// 	return reply.TaskType, reply.taskFiles, nil
	// } else {
	// 	return 0, nil, err
	// }
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	return err
}
