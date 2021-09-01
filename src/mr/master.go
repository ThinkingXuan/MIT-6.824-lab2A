package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	status int
	fileName string     // for map process, just id
	fileIdx int			// for map process, just id
	partIdx int			// for reduece process, just id
	nReduce int
	nFiles int
}

type TaskInterface interface {
	GetTaskInfo() TaskInfo
	GetTask() Task
	GetStatus() int
	SetStatus(int)
	GetFileName() string
	GetFileIdx() int
	GetPartIdx() int
	SetPartIdx(int)
	GetNReduce() int
	GetNFiles() int
}

func (this *Task) GetTaskInfo() TaskInfo {
	return TaskInfo{
		Status: this.status,
		FileName: this.fileName,
		FileIdx: this.fileIdx,
		PartIdx: this.partIdx,
		NReduce: this.nReduce,
		NFiles: this.nFiles,
	}
}

func (this *Task) GetTask() Task {
	return *this
}

func (this *Task) GetStatus() int {
	return this.status
}

func (this *Task) SetStatus(status int)  {
	this.status = status
}

func (this *Task) GetFileName() string {
	return this.fileName
}

func (this *Task) GetFileIdx() int {
	return this.fileIdx
}

func (this *Task) GetPartIdx() int {
	return this.partIdx
}

func (this *Task) SetPartIdx(partIdx int)  {
	this.partIdx = partIdx
}

func (this *Task) GetNReduce() int {
	return this.nReduce
}

func (this *Task) GetNFiles() int {
	return this.nFiles
}

type TaskQueue struct {
	queue []TaskInterface
	mutex sync.Mutex
}

func (this *TaskQueue) Lock() {
	this.mutex.Lock()
}

func (this *TaskQueue) Unlock() {
	this.mutex.Unlock()
}

func (this *TaskQueue) Size() int {
	return len(this.queue)
}

func (this *TaskQueue) Pop() TaskInterface {
	this.Lock()
	if this.Size() == 0 {
		this.Unlock()
		return nil
	}
	ret := this.queue[0]
	this.queue = this.queue[1:this.Size()]
	this.Unlock()
	return ret
}

func (this *TaskQueue) Push(task TaskInterface) {
	this.Lock()
	this.queue = append(this.queue, task)
	this.Unlock()
}

type Master struct {
	// Your definitions here.
	fileNames []string
	// task queue
	taskQueue TaskQueue
	// is done?
	isDone bool
	// the number of reduce task
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (this *Master) AskTask(args *ExampleArgs, reply *TaskInfo) error {
	if this.isDone {
		reply.Status = TasksAllDone
		return nil
	}
	// pick one task for worker
	task := this.taskQueue.Pop()
	if task != nil {
		//this.taskQueue.Push(task)
		*reply = TaskInfo{
			Status: task.GetStatus(),
			FileName: task.GetFileName(),
			FileIdx: task.GetFileIdx(),
			PartIdx: task.GetPartIdx(),
			NReduce: task.GetNReduce(),
			NFiles: task.GetNFiles(),
		}
	} else {	// all tasks completed
		reply.Status = TasksAllDone
		this.isDone = true
	}
	return nil
}

// receive from worker: the task is done
func (this *Master) TaskDone(taskInfo *TaskInfo, reply *ExampleArgs) error {
	switch taskInfo.Status {
	case TaskMap:
		fmt.Printf("mapTask of %vth file %v completed\n", taskInfo.FileIdx, taskInfo.FileName)
		// each mapTask divided into nReduce reduceTasks
		for i:=0; i<taskInfo.NReduce; i++ {
			task := Task{
				status: TaskReduce,
				fileIdx: taskInfo.FileIdx,
				fileName: taskInfo.FileName,
				partIdx: i,
				nReduce: taskInfo.NReduce,
				nFiles: taskInfo.NFiles,
			}
			this.taskQueue.Push(&task)
		}
		break
	case TaskReduce:
		fmt.Printf("reduceTask of %vth part %vth file %v completed\n", taskInfo.PartIdx, taskInfo.FileIdx, taskInfo.FileName)
		break
	default:
		panic("Task Done error")
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	return m.isDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	queue := make([]TaskInterface, 0)
	for fileIdx, fileName := range files {
		mapTak := Task {
			fileName:  fileName,
			fileIdx: fileIdx,
			nReduce: nReduce,
			partIdx: 0,
			nFiles: len(files),
		}
		queue = append(queue, &mapTak)
	}
	m := Master{
		taskQueue: TaskQueue{queue: queue},
		nReduce: nReduce,
		fileNames: files,
	}
	m.server()
	return &m
}
