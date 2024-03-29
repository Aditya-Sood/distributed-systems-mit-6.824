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

type TaskState int

const (
	Pending TaskState = iota
	InProgress
	Completed
)

type TaskConfig struct {
	ID               string
	State            TaskState
	AssignedWorkerID string
	StartTime        time.Time
	EndTime          time.Time
}

type MapTaskDetails struct {
	Filename           string `json:"filename"`
	Contents           string `json:"contents"`
	ReducePartitionsCt int    `json:"reduce_partitions_count"`
}

type ReduceTaskDetails struct {
	ReducePartitionID int      `json:"reduce_partition_id"`
	InputFilesSet     []string `json:"input_intermediate_files"` //list of input intermediate (k,v) files
}

const (
	MapTaskDetailsJsonStringTemplate    = "{\"filename\":\"%v\", \"contents\": \"%v\", \"reduce_partition_count\": %v}"
	ReduceTaskDetailsJsonStringTemplate = "{\"reduce_parition_id\": %v}"
)

type MapTask struct {
	TaskConfig
	MapTaskDetails
	OutputIntermediateFiles map[int]string //map so that each parition's intermediate file can be found using key
}

type ReduceTask struct {
	TaskConfig
	ReduceTaskDetails
}

type Coordinator struct {
	// Your definitions here.
	InputFiles []string
	MTasks     []MapTask
	MMtx       sync.RWMutex
	RTasks     []ReduceTask
	RMtx       sync.RWMutex
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
	ret = c.areAllTasksComplete()
	// log.Println("Done is", ret)

	return ret
}

func (c *Coordinator) areAllTasksComplete() bool {
	c.MMtx.RLock()
	defer c.MMtx.RUnlock()
	for _, mTask := range c.MTasks {
		if mTask.State != Completed {
			return false
		}
	}

	c.RMtx.RLock()
	defer c.RMtx.RUnlock()
	for _, rTask := range c.RTasks {
		if rTask.State != Completed {
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
		InputFiles: append([]string{}, files...),
		MTasks:     make([]MapTask, len(files)),
		RTasks:     make([]ReduceTask, nReduce),
	}

	// Your code here.
	//Create mapper tasks
	for ind, filename := range c.InputFiles {
		c.MTasks[ind] = MapTask{
			TaskConfig: TaskConfig{
				ID:    strconv.Itoa(ind),
				State: Pending,
			},
			MapTaskDetails: MapTaskDetails{
				Filename:           filename,
				Contents:           string(GetFileContent(filename)),
				ReducePartitionsCt: nReduce,
			},
			OutputIntermediateFiles: make(map[int]string),
		}

		log.Println("created map task", c.MTasks[ind].ID)
	}

	//Create reducer tasks
	for i := 0; i < nReduce; i++ {
		c.RTasks[i] = ReduceTask{
			TaskConfig: TaskConfig{
				ID:    strconv.Itoa(i),
				State: Pending,
			},
			ReduceTaskDetails: ReduceTaskDetails{
				ReducePartitionID: i,
				InputFilesSet:     []string{},
			},
		}

		log.Println("created reduce task", c.RTasks[i].ID)
	}

	log.Println("created Coordinator object, returning control to mrcoordinator.go ...")

	c.server()
	return &c
}

func (c *Coordinator) AssignTaskToWorkerProcess(args *AssignTaskToWorkerProcessRequest, reply *AssignTaskToWorkerProcessReply) error {

	//Check for available mapper tasks
	reply.AllTasksCompleted = false
	reply.IsMapTask = true
	mapTasksInProgress := false

	avlTaskInd := -1

	c.MMtx.Lock()
	defer c.MMtx.Unlock()
	for ind := range c.MTasks {
		mapTasksInProgress = (mapTasksInProgress || (c.MTasks[ind].State == InProgress))

		if c.MTasks[ind].State == Pending {
			avlTaskInd = ind
			break
		}
	}

	if avlTaskInd == -1 && mapTasksInProgress {
		//No map tasks are pending but one or more are in-progress, so can't start reducer phase
		//Hence not assigning new tasks

		reply.TaskID = ""
		reply.TaskDetailsJsonString = ""
		reply.AllTasksCompleted = false

		return nil

	} else if avlTaskInd != -1 {
		log.Printf("found pending mapper task %v\n", avlTaskInd)

		//Set coordinator values
		c.MTasks[avlTaskInd].AssignedWorkerID = args.WorkerID
		c.MTasks[avlTaskInd].StartTime = time.Now()
		c.MTasks[avlTaskInd].State = InProgress

		//Prepare reply for worker
		reply.TaskID = c.MTasks[avlTaskInd].ID
		taskStruct := c.MTasks[avlTaskInd].MapTaskDetails

		marshalOp, marshalErr := json.Marshal(taskStruct)

		if marshalErr != nil {
			log.Printf("encountered error while trying marshal task: %v", marshalErr)
			return marshalErr
		} else {
			reply.TaskDetailsJsonString = string(marshalOp)
		}

		log.Printf("assigning mapper task %v to worker (ID) %v\n", c.MTasks[avlTaskInd].Filename, c.MTasks[avlTaskInd].AssignedWorkerID)

		go c.checkTaskResult(&c.MTasks[avlTaskInd])

		return nil
	}

	//No pending mapper tasks exist at this point
	//Check for available reducer tasks
	reply.IsMapTask = false
	reduceTasksInProgress := false

	avlTaskInd = -1

	c.RMtx.Lock()
	defer c.RMtx.Unlock() //Doesn't cause deadlock with ReceiveTaskCompletionUpdate
	for ind := range c.RTasks {
		reduceTasksInProgress = (reduceTasksInProgress || (c.RTasks[ind].State == InProgress))

		if c.RTasks[ind].State == Pending {
			avlTaskInd = ind
			break
		}
	}

	if avlTaskInd == -1 && reduceTasksInProgress {
		//No reduce tasks are pending but one or more are in-progress
		//So can't end worker
		reply.TaskID = ""
		reply.TaskDetailsJsonString = ""
		reply.AllTasksCompleted = false

		return nil

	} else if avlTaskInd != -1 {
		log.Printf("found pending reducer task %v\n", c.RTasks[avlTaskInd].ID)

		//Set coordinator values
		c.RTasks[avlTaskInd].AssignedWorkerID = args.WorkerID
		c.RTasks[avlTaskInd].StartTime = time.Now()
		c.RTasks[avlTaskInd].State = InProgress

		//Prepare reply for worker
		reply.TaskID = c.RTasks[avlTaskInd].ID

		reducerTaskPartitionID := c.RTasks[avlTaskInd].ReducePartitionID
		intermediateFiles := make([]string, 0)

		for _, mTask := range c.MTasks {
			filepath := mTask.OutputIntermediateFiles[reducerTaskPartitionID]
			if filepath != "" { // as it's possible for there to be no input intermediate files for a particular parition ID
				intermediateFiles = append(intermediateFiles, filepath)
			}
		}

		taskStruct := ReduceTaskDetails{
			ReducePartitionID: reducerTaskPartitionID,
			InputFilesSet:     intermediateFiles,
		}

		marshalOp, marshalErr := json.Marshal(taskStruct)

		if marshalErr != nil {
			log.Fatalf("encountered error while trying to marshal task struct to JSON: %v", marshalErr)
		} else {
			reply.TaskDetailsJsonString = string(marshalOp)
		}

		log.Printf("assigning reducer task %v\n", c.RTasks[avlTaskInd].ID)

		go c.checkTaskResult(&c.RTasks[avlTaskInd])

		return nil
	}

	reply.AllTasksCompleted = true //no pending or in-progress tasks left, can end worker processes
	return nil
}

func (c *Coordinator) checkTaskResult(task interface{}) {
	//increased task completion timeout period from 10s since tasks are always alive when reset, plus a framework-accurate heartbeat model would behave the same
	time.Sleep(60 * time.Second)

	var taskCfg *TaskConfig
	var taskMtx *sync.RWMutex
	var taskType string

	if mTask, ok := task.(*MapTask); ok {
		taskCfg = &(mTask.TaskConfig)
		taskMtx = &c.MMtx
		taskType = "map"

	} else if rTask, ok := task.(*ReduceTask); ok {
		taskCfg = &(rTask.TaskConfig)
		taskMtx = &c.RMtx
		taskType = "reduce"

	} else {
		log.Fatalf("ERROR: received value is of unexpected type %T\n", task)
		return
	}

	log.Printf("checking status of %v task %v\n", taskType, taskCfg.ID)

	taskMtx.Lock()
	defer taskMtx.Unlock()
	if taskCfg.State != Completed {
		log.Printf("%v task %v not completed, resetting...\n", taskType, taskCfg.ID)

		taskCfg.StartTime = time.Time{}
		taskCfg.State = Pending
		taskCfg.AssignedWorkerID = ""
	}
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

func (c *Coordinator) ReceiveTaskCompletionUpdate(args *TaskCompletionUpdateRequest, reply *TaskCompletionUpdateReply) error {
	//copy arguments from args
	workerID := args.WorkerID
	isMapTask := args.IsMapTask
	taskId := args.TaskID
	status := args.State
	outputFiles := args.OutputFiles

	log.Printf("received status update from worker %v for task ID %v\n", workerID, taskId)

	if isMapTask {
		c.MMtx.Lock()
		defer c.MMtx.Unlock()

		tasksList := c.MTasks

		for ind, mapTask := range tasksList {
			// log.Printf("comparing received task ID %v with task %v - %v\n", taskId, mapTask.ID, mapTask.Filename)

			if mapTask.ID != taskId {
				continue
			}

			// log.Println("found matching task")

			if mapTask.State != InProgress {
				log.Printf("ERROR: task is in state %v, not valid to recieve a completion update", mapTask.State)
				return errors.New("task not in-progress, invalid completion update")

			} else if mapTask.AssignedWorkerID != workerID {
				log.Printf("stale update received: worker %v is no longer the assigned worker, status update to be received from %v\n", workerID, mapTask.AssignedWorkerID)
				return errors.New("worker task considered to have timed-out, status not updated")
			}

			log.Printf("status update for map task %v - %v\n", taskId, status)

			c.MTasks[ind].State = status

			if status == Completed {
				c.MTasks[ind].OutputIntermediateFiles = outputFiles
			}

			break
		}

	} else {
		c.RMtx.Lock()
		defer c.RMtx.Unlock()

		tasksList := c.RTasks

		for ind, reduceTask := range tasksList {
			// log.Printf("comparing received task ID %v with task %v - %v\n", taskId, reduceTask.ID, reduceTask.ReducePartitionID)

			if reduceTask.ID != taskId {
				continue
			}

			// log.Println("found matching task")

			if reduceTask.State != InProgress {
				log.Printf("ERROR: task is in state %v, not valid to recieve a completion update", reduceTask.State)
				return errors.New("task not in-progress, invalid completion update")

			} else if reduceTask.AssignedWorkerID != workerID {
				log.Printf("stale update received: worker %v is no longer the assigned worker, status update to be received from %v\n", workerID, reduceTask.AssignedWorkerID)
				return errors.New("worker task considered to have timed-out, status not updated")
			}

			log.Printf("status update for reduce task %v - %v\n", taskId, status)

			c.RTasks[ind].State = status

			if status == Completed {
				log.Println("reduce output stored in", outputFiles[0])
			}

			break
		}
	}

	reply.Message = "finished updating task status"

	return nil
}
