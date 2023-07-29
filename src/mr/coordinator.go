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
	Pending TaskState = iota
	InProgress
	Completed
)

type TaskConfig struct {
	ID        string
	StartTime time.Time
	//TODO: EndTime time.Time
	State          TaskState
	AssignedWorker WorkerHandle
	MapTask        *MapTask
	ReduceTask     *ReduceTask
}

// Store task details (filename + contents for Mapper; reduce partition ID for Reducer)
// and use a flag to decide whether to parse as a mapper or reducer task

type MapTask struct {
	Filename string
	Contents string
}

type ReduceTask struct {
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
	//Location of results, state of tasks
	InputFiles        []string
	ReducePartitionCt int
	MapperCfgs        []TaskConfig
	MtxMapper         sync.Mutex
	ReducerCfgs       []TaskConfig
	MtxReducer        sync.Mutex
}

//TODO: Start Reducer and keep it reading until all mappers complete, and then start

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
	isTaskPending := checkForPendingTasks(c.MapperCfgs, &c.MtxMapper) || checkForPendingTasks(c.ReducerCfgs, &c.MtxReducer)
	ret = !isTaskPending
	// log.Println("Done is ", ret)

	return ret
}

func checkForPendingTasks(tasks []TaskConfig, taskMutex *sync.Mutex) bool {
	isTaskPending := false

	taskMutex.Lock()
	for i := 0; !isTaskPending && i < len(tasks); i++ {
		isTaskPending = (tasks[i].getTaskState() != Completed)
	}
	taskMutex.Unlock()

	return isTaskPending
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles:        append([]string{}, files...),
		ReducePartitionCt: nReduce,
		MapperCfgs:        make([]TaskConfig, len(files)),
		ReducerCfgs:       make([]TaskConfig, nReduce),
	}

	// Your code here.
	//Create mapper tasks
	for ind, filename := range c.InputFiles {
		c.MapperCfgs[ind].ID = strconv.Itoa(ind)
		c.MapperCfgs[ind].State = Pending
		c.MapperCfgs[ind].MapTask = &MapTask{Filename: filename, Contents: string(GetFileContent(filename))}

		log.Println("created task ", c.MapperCfgs[ind])
	}

	//Create proposed reducer tasks
	for i := 0; i < nReduce; i++ {
		c.ReducerCfgs[i].ID = strconv.Itoa(i + 1)
		c.ReducerCfgs[i].State = Pending
		c.ReducerCfgs[i].ReduceTask = &ReduceTask{ReducePartitionID: i + 1}
	}

	// log.Println("created Coordinator object, returning control to mrcoordinator.go ...")

	c.server()
	return &c
}

func (c *Coordinator) RequestTaskFromCoordinator(args *RequestTaskFromCoordinatorArgs, reply *RequestTaskFromCoordinatorReply) error {
	// TODO: Check for errors and return appropriately below

	//Check for available mapper tasks
	reply.AllTasksCompleted = false
	reply.IsMapTask = true
	mapTasksInProgress := false

	avlTaskInd := -1

	c.MtxMapper.Lock()
	defer c.MtxMapper.Unlock()
	for ind := range c.MapperCfgs {
		mapTasksInProgress = (mapTasksInProgress || (c.MapperCfgs[ind].State == InProgress))

		if c.MapperCfgs[ind].State == Pending {
			avlTaskInd = ind
			break
		}
	}

	if avlTaskInd == -1 && mapTasksInProgress {
		//No map tasks are pending but one or more are in-progress
		//So can't start reducer phase

		reply.TaskID = ""
		reply.TaskDetailsJsonString = ""

		return nil

	} else if avlTaskInd != -1 {
		log.Printf("found pending mapper task %v\n", avlTaskInd)

		c.MapperCfgs[avlTaskInd].AssignedWorker = args.Worker
		c.MapperCfgs[avlTaskInd].StartTime = time.Now()
		c.MapperCfgs[avlTaskInd].State = InProgress

		reply.TaskID = c.MapperCfgs[avlTaskInd].ID
		taskStruct := MapTaskDetails{
			Filename:          c.MapperCfgs[avlTaskInd].MapTask.Filename,
			Contents:          c.MapperCfgs[avlTaskInd].MapTask.Contents,
			ReducePartitionCt: c.ReducePartitionCt,
		}

		marshalOp, marshalErr := json.Marshal(taskStruct)

		if marshalErr != nil {
			log.Printf("encountered error while trying marshal task: %v", marshalErr)
			return marshalErr
		} else {
			reply.TaskDetailsJsonString = string(marshalOp)
		}

		log.Printf("assigning mapper task %v to worker (ID) %v\n", c.MapperCfgs[avlTaskInd].MapTask.Filename, c.MapperCfgs[avlTaskInd].AssignedWorker.ID)

		go checkTaskResult(&c.MapperCfgs[avlTaskInd], &c.MtxMapper)

		return nil
	}

	//TODO: Fix based on logic at the top
	//Check for available reducer tasks
	reply.IsMapTask = false
	reduceTasksInProgress := false

	avlTaskInd = -1

	c.MtxReducer.Lock()
	for ind := range c.ReducerCfgs {
		reduceTasksInProgress = (reduceTasksInProgress || (c.ReducerCfgs[ind].State == InProgress))

		if c.ReducerCfgs[ind].State == Pending {
			avlTaskInd = ind
			break
		}
	}
	// // log.Println("Unlocking at 257")
	c.MtxReducer.Unlock()

	if avlTaskInd != -1 {
		// log.Printf("found pending reducer task %v\n", availableReduceTask)

		// // log.Println("Locking at 263")
		c.MtxReducer.Lock()
		c.ReducerCfgs[avlTaskInd].AssignedWorker = args.Worker
		c.ReducerCfgs[avlTaskInd].StartTime = time.Now()
		c.ReducerCfgs[avlTaskInd].State = InProgress
		// // log.Println("Unlocking at 268")
		c.MtxReducer.Unlock()

		reply.TaskID = c.ReducerCfgs[avlTaskInd].ID
		taskStruct := ReduceTask{
			ReducePartitionID: c.ReducerCfgs[avlTaskInd].ReduceTask.ReducePartitionID,
		}

		marshalOp, marshalErr := json.Marshal(taskStruct)

		if marshalErr != nil {
			log.Fatalf("encountered error while trying to marshal task struct to JSON: %v", marshalErr)
		} else {
			reply.TaskDetailsJsonString = string(marshalOp)
		}

		log.Printf("assigning reducer task %v\n", c.ReducerCfgs[avlTaskInd])

		go checkTaskResult(&c.ReducerCfgs[avlTaskInd], &c.MtxReducer)

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
	time.Sleep(40 * time.Second) //Since task completion timeout period is 10 seconds

	log.Printf("checking status of task %v\n", taskConfig.ID)

	taskMutex.Lock()
	if taskConfig.State != Completed {
		log.Printf("task %v not completed, resetting...\n", taskConfig.ID)

		taskConfig.StartTime = time.Time{}
		taskConfig.State = Pending
		taskConfig.AssignedWorker = WorkerHandle{}
	}
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
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

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

	log.Printf("received status update from worker %v for task ID %v\n", workerID, taskId)

	if isMapTask {
		// // log.Println("Locking at 351")
		c.MtxMapper.Lock()
		// // log.Println("Unlocking at 353")
		defer c.MtxMapper.Unlock()

		tasksList := c.MapperCfgs

		for ind, mapTask := range tasksList {
			log.Printf("checking on task %v - %v\n", mapTask.MapTask.Filename, mapTask)

			if mapTask.ID != taskId {
				log.Printf("warn: %v doesn't match required task ID %v\n", mapTask.ID, taskId)
				continue
			}

			log.Println("found matching task")

			if mapTask.AssignedWorker.ID != workerID {
				log.Printf("stale update received: worker %v is no longer the assigned worker, status update to be received from %v\n", workerID, mapTask.AssignedWorker.ID)
				return errors.New("worker task considered to have timed-out, status not updated")
			}

			log.Printf("status update for map task %v - %v\n", taskId, status)

			c.MapperCfgs[ind].State = status

			break
		}

	} else {
		// // log.Println("Locking at 381")
		c.MtxReducer.Lock()
		// // log.Println("Unlocking at 383")
		defer c.MtxReducer.Unlock()

		tasksList := c.ReducerCfgs

		for ind, reduceTask := range tasksList {
			log.Printf("checking on task %v - %v\n", reduceTask.ReduceTask.ReducePartitionID, reduceTask)

			if reduceTask.ID != taskId {
				log.Printf("warn: %v doesn't match required task ID %v", reduceTask.ID, taskId)
				continue
			}

			log.Println("found matching task")

			if reduceTask.AssignedWorker.ID != workerID {
				log.Printf("stale update received: worker %v is no longer the assigned worker, status update to be received from %v\n", workerID, reduceTask.AssignedWorker.ID)
				return errors.New("worker task considered to have timed-out, status not updated")
			}

			log.Printf("status update for reduce task %v - %v\n", taskId, status)

			c.ReducerCfgs[ind].State = status

			break
		}
	}

	reply.Message = "finished updating task status"

	return nil
}
