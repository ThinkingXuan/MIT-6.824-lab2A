package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func CallAskTask() *TaskInfo {
	args := ExampleArgs{}
	reply := TaskInfo{}
	call("Master.AskTask", &args, &reply)
	return &reply
}

func CallTaskDone(taskInfo *TaskInfo) {
	reply := ExampleArgs{}
	call("Master.TaskDone", taskInfo, &reply)
}

// map process of worker
func WorkerMap(mapf func(string, string) []KeyValue, taskInfo *TaskInfo) {
	// logical business
	fmt.Printf("Got assigned map task on %vth file %v\n", taskInfo.FileIdx, taskInfo.FileName)
	// notify master: the mapTask is done
	CallTaskDone(taskInfo)
	time.Sleep(time.Duration(time.Second*2))
}

// reduce process of worker
func WorkerReduce(reducef func(string, []string) string, taskInfo *TaskInfo) {
	// logical business
	fmt.Printf("Got assigned reduce task on part %v of %vth file %v\n", taskInfo.PartIdx, taskInfo.FileIdx, taskInfo.FileName)
	// notify master: the reduceTask is done
	CallTaskDone(taskInfo)
	time.Sleep(time.Duration(time.Second*2))
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		taskInfo := CallAskTask()
		switch taskInfo.Status {
		case TaskMap:
			WorkerMap(mapf, taskInfo)
			break
		case TaskReduce:
			WorkerReduce(reducef, taskInfo)
			break
		case TasksAllDone:
			fmt.Println("All Tasks completed, nothing to do.")
			return
		default:
			panic("Invalid task status received from master.")
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
