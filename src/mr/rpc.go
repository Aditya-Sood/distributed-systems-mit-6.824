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

// Add your RPC definitions here.
type AssignTaskToWorkerProcessRequest struct {
	WorkerID string
}

type AssignTaskToWorkerProcessReply struct {
	TaskID                string `json:"task_id"`
	IsMapTask             bool   `json:"is_map_task"`
	TaskDetailsJsonString string `json:"task_details_json_string"`
	AllTasksCompleted     bool   `json:"tasks_left"`
}

type TaskCompletionUpdateRequest struct {
	WorkerID    string //since multiple workers could pick up the same task, need an identifier to confirm that only changes from the current task owner are picked
	IsMapTask   bool
	TaskID      string
	State       TaskState
	OutputFiles map[int]string
}

type TaskCompletionUpdateReply struct {
	Message string
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
