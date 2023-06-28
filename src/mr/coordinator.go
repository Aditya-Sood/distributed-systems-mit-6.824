package mr

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// go run -race mrcoordinator.go pg-*.txt

const (
	MapperIntermediateFilenameTemplate        = "mr-%v-%v"
	MapperIntermediateFilenameTemplatePattern = "mr-?*-%v"
	ReducerFinalOutputFilenameTemplate        = "mr-out-%v"
)

type TaskState int

const (
	Pending    TaskState = -1
	InProgress TaskState = 0
	Completed  TaskState = 1
)

type TaskConfig struct {
	ID             string
	StartTime      time.Time
	State          TaskState
	AssignedWorker WorkerHandle
}

// Store task details (filename + contents for Mapper; reduce partition ID for Reducer)
// and use a flag to decide whether to parse as a mapper or reducer task

type MapTask struct {
	TaskConfig
	Filename string
	Contents string
}

type ReduceTask struct {
	TaskConfig
	ReducePartitionID int
}

type TaskWithState interface {
	getTaskState() TaskState
}

func (c *TaskConfig) getTaskState() TaskState {
	return c.State
}

type Coordinator struct {
	// Your definitions here.
	InputFiles        []string
	ReducePartitionCt int
	MapperTasks       []MapTask
	MtxMapper         sync.Mutex
	ReducerTasks      []ReduceTask
	MtxReducer        sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	mapperTaskPending := checkForPendingTasks(c.MapperTasks.(TaskConfig), &c.MtxMapper)
	reducerTaskPending := false

	c.MtxMapper.Lock()
	for _, mapperTask := range c.MapperTasks {
		mapperTaskPending = mapperTaskPending || (mapperTask.State != Completed)
	}
	c.MtxMapper.Unlock()

	if mapperTaskPending {
		return ret
	}

	c.MtxReducer.Lock()
	for _, reducerTask := range c.ReducerTasks {
		reducerTaskPending = reducerTaskPending || (reducerTask.State != Completed)
	}
	c.MtxReducer.Unlock()

	if reducerTaskPending {
		return ret
	}

	ret = true
	return ret
}

func checkForPendingTasks(tasks []TaskWithState, taskMutex *sync.Mutex) bool {
	tasksPending := false

	taskMutex.Lock()
	for _, t := range tasks {
		tasksPending = tasksPending || (t.getTaskState() != Completed)
	}
	taskMutex.Unlock()

	return tasksPending
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles:        append([]string{}, files...),
		ReducePartitionCt: nReduce,
		MapperTasks:       make([]MapTask, len(files)),
		ReducerTasks:      make([]ReduceTask, nReduce),
	}

	// Your code here.
	//Create mapper tasks
	for ind, filename := range c.InputFiles {
		c.MapperTasks[ind].ID = strconv.Itoa(ind)
		c.MapperTasks[ind].Filename = filename
		c.MapperTasks[ind].Contents = string(GetFileContent(filename))
		c.MapperTasks[ind].State = Pending
	}

	//Create proposed reducer tasks
	for i := 0; i < nReduce; i++ {
		c.ReducerTasks[i].TaskConfig.ID = strconv.Itoa(i + 1)
		c.ReducerTasks[i].ReducePartitionID = i + 1
		c.ReducerTasks[i].State = Pending
	}

	// log.Println("created Coordinator object, returning control to mrcoordinator.go ...")

	c.server()
	return &c
}

func (c *Coordinator) RequestTaskFromCoordinator(args *RequestTaskFromCoordinatorArgs, reply *RequestTaskFromCoordinatorReply) error {
	// TODO: Check for errors and return appropriately below

	reply.AllTasksCompleted = false

	//Check for available mapper tasks
	reply.IsMapTask = true
	var availableMapTask *MapTask
	mapTasksInProgress := false

	// // log.Println("Locking at 186")
	c.MtxMapper.Lock()
	for ind := range c.MapperTasks {
		mapTasksInProgress = (mapTasksInProgress || (c.MapperTasks[ind].State == InProgress))

		if c.MapperTasks[ind].State == Pending {
			availableMapTask = &c.MapperTasks[ind]
			break
		}
	}
	// // log.Println("Unlocking at 196")
	c.MtxMapper.Unlock()

	if availableMapTask != nil {
		// log.Printf("found pending mapper task %v\n", availableMapTask.Filename)

		// // log.Println("Locking at 202")
		c.MtxMapper.Lock()
		availableMapTask.AssignedWorker = *args.Worker
		availableMapTask.StartTime = time.Now()
		availableMapTask.State = InProgress
		// // log.Println("Unlocking at 207")
		c.MtxMapper.Unlock()

		reply.TaskID = availableMapTask.ID
		taskStruct := MapTaskDetails{
			Filename:          availableMapTask.Filename,
			Contents:          availableMapTask.Contents,
			ReducePartitionCt: c.ReducePartitionCt,
		}

		marshalOp, marshalErr := json.Marshal(taskStruct)

		if marshalErr != nil {
			log.Fatalf("encountered error while trying marshal task: %v", marshalErr)
		} else {
			reply.TaskDetailsJsonString = string(marshalOp)
		}

		// log.Printf("assigning mapper task %v to worker (ID) %v\n", availableMapTask.Filename, availableMapTask.AssignedWorker.ID)

		go checkTaskResult(&availableMapTask.TaskConfig, &c.MtxMapper)

		return nil
	}

	if mapTasksInProgress {
		//No map tasks are pending but one or more are in-progress
		//So can't start reducer phase

		reply.TaskID = ""
		reply.TaskDetailsJsonString = ""

		return nil
	}

	//Check for available reducer tasks
	reply.IsMapTask = false
	var availableReduceTask *ReduceTask
	reduceTasksInProgress := false

	// // log.Println("Locking at 247")
	c.MtxReducer.Lock()
	for ind := range c.ReducerTasks {
		reduceTasksInProgress = (reduceTasksInProgress || (c.ReducerTasks[ind].State == InProgress))

		if c.ReducerTasks[ind].State == Pending {
			availableReduceTask = &c.ReducerTasks[ind]
			break
		}
	}
	// // log.Println("Unlocking at 257")
	c.MtxReducer.Unlock()

	if availableReduceTask != nil {
		// log.Printf("found pending reducer task %v\n", availableReduceTask)

		// // log.Println("Locking at 263")
		c.MtxReducer.Lock()
		availableReduceTask.AssignedWorker = *args.Worker
		availableReduceTask.StartTime = time.Now()
		availableReduceTask.State = InProgress
		// // log.Println("Unlocking at 268")
		c.MtxReducer.Unlock()

		reply.TaskID = availableReduceTask.ID
		taskStruct := ReduceTask{
			ReducePartitionID: availableReduceTask.ReducePartitionID,
		}

		marshalOp, marshalErr := json.Marshal(taskStruct)

		if marshalErr != nil {
			log.Fatalf("encountered error while trying to marshal task struct to JSON: %v", marshalErr)
		} else {
			reply.TaskDetailsJsonString = string(marshalOp)
		}

		// log.Printf("assigning reducer task %v\n", availableReduceTask)

		go checkTaskResult(&availableReduceTask.TaskConfig, &c.MtxReducer)

		return nil
	}

	//No pending (mapper or reducer) tasks exist
	if reduceTasksInProgress {
		//No reduce tasks are pending but one or more are in-progress
		//So can't end worker
		reply.TaskID = ""
		reply.TaskDetailsJsonString = ""

	} else {
		reply.AllTasksCompleted = true //no pending or in-progress tasks left, can end worker processes
	}

	return nil
}

func checkTaskResult(taskConfig *TaskConfig, taskMutex *sync.Mutex) {
	time.Sleep(10 * time.Second) //Since task completion timeout period is 10 seconds

	// log.Printf("checking status of task %v\n", taskConfig.ID)

	// // log.Println("Locking at 310")
	taskMutex.Lock()
	if taskConfig.State != Completed {
		// log.Printf("task %v not completed, resetting...\n", taskConfig.ID)

		taskConfig.ID = ""
		taskConfig.StartTime = time.Time{}
		taskConfig.State = Pending
		taskConfig.AssignedWorker = WorkerHandle{}
	}
	// // log.Println("Unlocking at 316")
	taskMutex.Unlock()
	/*
	*
	*
	*
	*
	 */
}

func GetFileContent(filename string) []byte {
	file, err := os.Open(filename)
	defer file.Close()

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	return content
}

func (c *Coordinator) UpdateCoordinatorOnTaskStatus(args *UpdateCoordinatorOnTaskStatusArgs, reply *UpdateCoordinatorOnTaskStatusReply) error {
	//copy arguments from args
	workerID := args.WorkerID
	isMapTask := args.IsMapTask
	taskId := args.TaskID
	status := args.State

	// log.Printf("received status update from worker %v for task ID %v\n", workerID, taskId)

	if isMapTask {
		// // log.Println("Locking at 351")
		c.MtxMapper.Lock()
		// // log.Println("Unlocking at 353")
		defer c.MtxMapper.Unlock()

		tasksList := c.MapperTasks

		for ind, mapTask := range tasksList {
			// log.Printf("checking on task %v - %v\n", mapTask.Filename, mapTask.config)

			if mapTask.TaskConfig.ID != taskId {
				// log.Printf("warn: %v doesn't match required task ID %v", mapTask.config.ID, taskId)
				continue
			}

			// log.Println("found matching task")

			if mapTask.AssignedWorker.ID != workerID {
				// log.Printf("stale update received: worker %v is no longer the assigned worker, status update to be received from %v\n", workerID, mapTask.AssignedWorker.ID)
				return errors.New("worker task considered to have timed-out, status not updated")
			}

			// log.Printf("status update for map task %v - %v\n", taskId, status)

			c.MapperTasks[ind].State = status

			break
		}

	} else {
		// // log.Println("Locking at 381")
		c.MtxReducer.Lock()
		// // log.Println("Unlocking at 383")
		defer c.MtxReducer.Unlock()

		tasksList := c.ReducerTasks

		for ind, reduceTask := range tasksList {
			// log.Printf("checking on task %v - %v\n", reduceTask.ReducePartitionID, reduceTask.config)

			if reduceTask.TaskConfig.ID != taskId {
				// log.Printf("warn: %v doesn't match required task ID %v", reduceTask.config.ID, taskId)
				continue
			}

			// log.Println("found matching task")

			if reduceTask.AssignedWorker.ID != workerID {
				// log.Printf("stale update received: worker %v is no longer the assigned worker, status update to be received from %v\n", workerID, reduceTask.AssignedWorker.ID)
				return errors.New("worker task considered to have timed-out, status not updated")
			}

			// log.Printf("status update for reduce task %v - %v\n", taskId, status)

			c.ReducerTasks[ind].State = status

			break
		}
	}

	reply.Message = "finished updating task status"

	return nil
}
