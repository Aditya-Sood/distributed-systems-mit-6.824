package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
)

//TODO: Deadlock exists for multi-worker tests - 4 to 5 workers triggerring deadlock

// go build -race -buildmode=plugin ../mrapps/wc.go && go run -race mrworker.go wc.so

/*
* Keep checking for tasks every timeout seconds (10 sec)
* Workers will stay up until manually killed (since the tasks go to Coordinator directly, and worker won't be involved)
* -> So a 'for' loop with Time.sleep(10*Time.Second)
 */

const (
	MapperIntermediateFilenameTemplate = "mr-%v-%v-%v"
	ReducerFinalOutputFilenameTemplate = "mr-out-%v"
)

type WorkerHandle struct {
	ID        string
	IsWorking bool
}

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

	//mapf works on (filename, contents) to return a slice of (k, v)
	//reducef works on (key, slice of key values) to return the reduced output for the key

	// Your worker implementation here.
	var workerHandle = WorkerHandle{ID: uuid.New().String(), IsWorking: false}
	// log.Printf("Started worker process %v...\n", workerHandle)

	for {
		time.Sleep(10 * time.Second)

		// log.Printf("worker %v is awake, requesting coordinator for task...\n", workerHandle.ID)
		coordinatorReply := workerHandle.requestTaskFromCoordinator()
		if coordinatorReply == nil || coordinatorReply.TaskDetailsJsonString == "" {
			continue
		}

		if coordinatorReply.AllTasksCompleted {
			break
		}

		outputFilepaths := make(map[int]string)

		if coordinatorReply.IsMapTask {
			// log.Printf("coordinator has assigned a mapper task\n")
			taskID := coordinatorReply.TaskID
			task := MapTaskDetails{}
			json.Unmarshal([]byte(coordinatorReply.TaskDetailsJsonString), &task)

			// log.Printf("running map function on input file %v ...\n", task.Filename)
			intermediateKV := mapf(task.Filename, task.Contents)

			sort.Sort(ByKey(intermediateKV))

			//write results to appropriate intermediate keys file(s)
			currentKey := intermediateKV[0].Key
			currentHash := ihash(currentKey) % task.ReducePartitionsCt
			currFilename := fmt.Sprintf(MapperIntermediateFilenameTemplate, workerHandle.ID, taskID, currentHash)
			outputFile, err := os.OpenFile(currFilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
			if err != nil {
				log.Fatalf("cannot create %v", currFilename)
			}
			encoder := json.NewEncoder(outputFile)

			// log.Printf("writing intermediate (key, value) results to file...\n")

			for _, kv := range intermediateKV {
				if kv.Key != currentKey {

					if err = outputFile.Close(); err != nil {
						log.Fatalf("failed to close output file %v: %v", outputFile.Name(), err)
					} else {
						outputFilepaths[currentHash] = outputFile.Name()
					}

					currentKey = kv.Key
					currentHash = ihash(currentKey) % task.ReducePartitionsCt
					currFilename = fmt.Sprintf(MapperIntermediateFilenameTemplate, workerHandle.ID, taskID, strconv.Itoa(currentHash))
					outputFile, err = os.OpenFile(currFilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
					if err != nil {
						log.Fatalf("cannot open %v", currFilename)
					}
					encoder = json.NewEncoder(outputFile)
				}

				err := encoder.Encode(&kv)
				if err != nil {
					log.Fatalf("unable to write intermediate keys to destination: %v", err)
				}
			}
			//since the last key won't trigger the if-else block above
			if err = outputFile.Close(); err != nil {
				log.Fatalf("failed to close output file %v: %v", outputFile.Name(), err)
			} else {
				outputFilepaths[currentHash] = outputFile.Name()
			}

			// log.Printf("completed writing intermediate mapper results to file\n")

		} else {
			// log.Printf("coordinator has assigned a reducer task\n")

			task := ReduceTaskDetails{}
			json.Unmarshal([]byte(coordinatorReply.TaskDetailsJsonString), &task)

			partitionID := task.ReducePartitionID

			// filenamePattern := fmt.Sprintf(MapperIntermediateFilenameTemplatePattern, partitionID)
			// log.Printf("searching for files matching intermediate output file(s) pattern %v ...\n", filenamePattern)
			intermediateFilepaths := task.InputFilesSet //, err := filepath.Glob("./" + filenamePattern)

			/* if err != nil {
				log.Fatalf("unable to find intermediate files in the present working directory: %v", err)
			} */

			// log.Printf("reading from intermediate (key, value) result files...\n")
			intermediateKVList := []KeyValue{}
			var file *os.File
			var openErr error
			for _, filepath := range intermediateFilepaths {
				//find relevant filepaths
				if file != nil {
					file.Close()
				}

				file, openErr = os.Open(filepath)

				if openErr != nil {
					log.Fatalf("unable to read from intermediate output file at %v: %v", filepath, openErr)
				}
				dec := json.NewDecoder(file)

				// log.Printf("reading file %v\n", filepath)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediateKVList = append(intermediateKVList, kv)
				}
			}

			if file != nil {
				file.Close()
			}

			if len(intermediateKVList) == 0 {
				continue
			}

			//process collected intermediate key values
			sort.Sort(ByKey(intermediateKVList))

			//process all values corresponding to one key
			curKey := intermediateKVList[0].Key
			curValues := []string{}
			filepath := fmt.Sprintf(ReducerFinalOutputFilenameTemplate, partitionID)
			outputFile, fileErr := os.OpenFile(filepath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)

			if fileErr != nil {
				log.Fatalf("unable to create reducer task output file %v: %v", filepath, fileErr)
			}

			for _, kv := range intermediateKVList {
				if kv.Key != curKey {
					//won't process the last key
					// log.Printf("running reduce function on values for key %v...\n", curKey)
					reductionResult := reducef(curKey, curValues)
					fmt.Fprintf(outputFile, "%v %v\n", curKey, reductionResult)
					curKey = kv.Key
					curValues = []string{}
				}
				curValues = append(curValues, kv.Value)
			}

			//processing the last key
			// log.Printf("running reduce function on values for key %v...\n", curKey)
			reductionResult := reducef(curKey, curValues)
			fmt.Fprintf(outputFile, "%v %v\n", curKey, reductionResult)

			if err := outputFile.Close(); err != nil {
				log.Fatalf("failed to close output file %v: %v", outputFile.Name(), err)
			} else {
				outputFilepaths[0] = outputFile.Name()
			}

			// log.Printf("completed writing final reducer output to file %v\n", outputFile)
		}

		//inform coordinator of task status
		callSuccessful := workerHandle.updateCoordinatorOnTaskCompletion(coordinatorReply.IsMapTask, coordinatorReply.TaskID, Completed, outputFilepaths)

		if !callSuccessful {
			for _, filepath := range outputFilepaths {
				if err := os.Remove(filepath); err != nil {
					log.Fatalf("failed to cleanup stale output file %v: %v", filepath, err)
				}
			}
		}
	}

	// log.Println("No pending tasks available with coordinator, shutting down worker process...")
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

func (w *WorkerHandle) requestTaskFromCoordinator() *AssignTaskToWorkerProcessReply {

	// declare an argument structure.
	args := AssignTaskToWorkerProcessRequest{WorkerID: w.ID}

	// declare a reply structure.
	reply := AssignTaskToWorkerProcessReply{}

	// send the RPC request, wait for the reply.
	if ok := call("Coordinator.AssignTaskToWorkerProcess", &args, &reply); ok {
		log.Printf("assigned task by coordinator: ID - %v, isMapTask - %v\n", reply.TaskID, reply.IsMapTask)
		return &reply
	} else {
		log.Println("rpc call to request task from coordinator failed")
		return nil
	}
}

func (w *WorkerHandle) updateCoordinatorOnTaskCompletion(isMapTask bool, taskID string, status TaskState, outputFilepaths map[int]string) bool {

	// declare an argument structure.
	// log.Println("updating coordinator on task completion")
	args := TaskCompletionUpdateRequest{
		WorkerID:    w.ID,
		IsMapTask:   isMapTask,
		TaskID:      taskID,
		State:       status,
		OutputFiles: outputFilepaths,
	}

	// fill in the argument(s).
	// log.Println("task status - ", args)

	// declare a reply structure.
	reply := TaskCompletionUpdateReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	if ok := call("Coordinator.ReceiveTaskCompletionUpdate", &args, &reply); ok {
		log.Printf("update from coordinator: %v\n", reply.Message)
		return true
	} else {
		log.Println("rpc call to notify task completion to coordinator failed")
		return false
	}
}
