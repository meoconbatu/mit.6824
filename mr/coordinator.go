package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type state string

const (
	// IDLE is a task that has not been assigned to a worker yet.
	IDLE state = "IDLE"
	// INPROGRESS is a task that has been assigned to a worker and is running.
	INPROGRESS = "INPROGRESS"
	// COMPLETED is a task that has been completed.
	COMPLETED = "COMPLETED"
)

type taskType string

const (
	// MAP is a task type.
	MAP taskType = "MAP"
	// REDUCE is a task type.
	REDUCE = "REDUCE"
	// EXIT is a task type
	EXIT = "EXIT"
)

// Task is a struct that contains information of a task.
type Task struct {
	Status   state
	FileName string // the file that the WorkerID needs to read/write
	WorkerID *int   // id of the worker that the task is assigned to
}

// Coordinator is a struct that contains the state of the coordinator.
type Coordinator struct {
	// Your definitions here.
	mapTasks    []Task
	reduceTasks []Task
	NReduce     int
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// Example func
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RequestTask func
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	for i, task := range c.mapTasks {
		if task.Status == IDLE {
			reply.FileName = task.FileName
			reply.TaskType = MAP
			reply.NReduce = c.NReduce
			c.mu.Lock()
			c.mapTasks[i].WorkerID = &args.WorkerID
			c.mapTasks[i].Status = INPROGRESS
			c.mu.Unlock()
			return nil
		}
	}
	reply.TaskType = EXIT
	return nil
}

// Notify func
func (c *Coordinator) Notify(args *RequestTaskArgs, reply *RequestTaskReply) error {
	for i, task := range c.mapTasks {
		if *task.WorkerID == args.WorkerID {
			c.mu.Lock()
			c.mapTasks[i].Status = args.Status
			c.mu.Unlock()
			return nil
		}
	}
	return errors.New("workerid not exists")
}

//
// start a thread that listens for RPCs from worker.go
//
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

// Done func
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	for _, task := range c.mapTasks {
		if task.Status != COMPLETED {
			c.mu.Unlock()
			return false
		}
	}
	c.mu.Unlock()
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := initMapTask(files)
	c := Coordinator{mapTasks, nil, nReduce, sync.Mutex{}}

	// Your code here.
	initMapTask(files)
	c.server()
	return &c
}
func initMapTask(files []string) []Task {
	tasks := make([]Task, len(files))
	for i, file := range files {
		tasks[i] = Task{IDLE, file, nil}
	}
	return tasks
}
