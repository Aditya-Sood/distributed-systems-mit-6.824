package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RequestTaskFromCoordinatorArgs struct {
	Worker *WorkerHandle
}

type RequestTaskFromCoordinatorReply struct {
	TaskID                string `json:task_id`
	IsMapTask             bool   `json: is_map_task`
	TaskDetailsJsonString string `json:task_details_json_string`
}

type UpdateCoordinatorOnTaskStatusArgs struct {
	WorkerID  string //since multiple workers could pick up the same task, need an identifier to confirm that only changes from the current task owner are picked
	IsMapTask bool
	TaskID    string
	State     TaskState
}

type UpdateCoordinatorOnTaskStatusReply struct {
	//NA? because just need to update task status
	Message string
}

// Add your RPC definitions here.

/**
* RPCs:
* 1. Submit task
	a. Channel -> Can't control which worker picks up the task -> Can't track runtime from coordinator's end
	b. RPC -> Use functions? -> Manually assign tasks to available workers
		-> Heartbeats from available workers until all tasks are completed
			-> Assign task to first available worker process (Maintain list in Coordinator)

		-> Loop checking on busy workers and their processing time (Timeout in 10 secs)
			-> List of assigned tasks -> Start time of each task
			-> If timeout period has passed, assign task to another worker

	1.1 -> Tast Creator | Channel | Task Assigner
	-> Task Assigner has to check if any failed tasks before assigning afresh? OR Mark the previous task as pending in Creators's list


* 2. Heartbeats from Workers (isWorking flag -> assign task if false)


*/

func SubmitTask(filename string, contents string) {

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
