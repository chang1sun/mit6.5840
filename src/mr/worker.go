package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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

	for {
		taskType, taskNo, nReduce, taskFiles, err := AskForTask()
		if err != nil {
			log.Printf("No reply, worker %v exits...\n", os.Getpid())
			os.Exit(0)
		}

		switch taskType {
		// Handle map task.
		case TaskTypeMap:
			log.Printf("Map task %v assigned", taskNo)
			filename := taskFiles[0]
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			mapResKva := mapf(filename, string(content))

			// group by reduceNo.
			grouped := make(map[int][]KeyValue)
			for _, kv := range mapResKva {
				groupKey := ihash(kv.Key) % nReduce
				grouped[groupKey] = append(grouped[groupKey], kv)
			}

			for reduceNo, kva := range grouped {
				tmpFile, err := os.CreateTemp(".", "map_tmp")
				if err != nil {
					log.Fatal(err)
				}

				encoder := json.NewEncoder(tmpFile)
				for _, kv := range kva {
					if err := encoder.Encode(kv); err != nil {
						log.Fatal(err)
					}
				}

				target := fmt.Sprintf("mr-%v-%v", taskNo, reduceNo)
				if err := os.Rename(tmpFile.Name(), target); err != nil {
					if os.IsExist(err) {
						if err := os.Remove(target); err != nil {
							log.Fatal(err)
						}
						if err := os.Rename(tmpFile.Name(), target); err != nil {
							log.Fatal(err)
						}
					} else {
						log.Fatal(err)
					}
				}
			}
			FinishTask(taskType, taskNo)

		// Handle reduce task.
		case TaskTypeReduce:
			log.Printf("Reduce task %v assigned", taskNo)
			intermediate := []KeyValue{}
			for _, filename := range taskFiles {
				file, err := os.Open(filename)
				if err != nil {
					if os.IsNotExist(err) {
						continue
					}
					log.Fatal(err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}

			sort.Sort(ByKey(intermediate))

			tmpFile, err := os.CreateTemp(".", "reduce_tmp")
			if err != nil {
				log.Fatal(err)
			}
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			if err := os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%v", taskNo)); err != nil {
				log.Fatal(err)
			}
			FinishTask(taskType, taskNo)

		case TaskTypeNon:
			log.Printf("No task assigned, wait and request again...")

		default:
			log.Fatalf("Invaild TaskType: %v", taskType)
		}

		// wait 50ms to continue.
		time.Sleep(50 * 1000 * 1000)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

/**
 * Ask for map/reduce tasks.
 */
func AskForTask() (TaskType, int, int, []string, error) {

	args := TaskAskArgs{
		os.Getpid(),
	}
	reply := TaskAskReply{}

	err := call("Coordinator.TaskAsk", &args, &reply)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	return reply.TaskType, reply.TaskNo, reply.NReduce, reply.Inputs, nil
}

/**
 * Finishs for map/reduce tasks.
 */
func FinishTask(taskType TaskType, taskNo int) {
	args := TaskFinishArgs{
		WorkerPid: os.Getpid(),
		TaskNo:    taskNo,
		TaskType:  taskType,
	}
	reply := TaskFinishReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	if err := call("Coordinator.TaskFinish", &args, &reply); err != nil {
		log.Fatal(err)
	}
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
