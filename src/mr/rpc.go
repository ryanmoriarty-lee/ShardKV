package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type TaskArgs struct{}
type TaskType int //the type of task
type Phase int    // the status of coordinator
type State int    // the status of task

const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask // if no task in pool, wait
	ExitTask     // if all task is done, exit
)

const (
	MapPhase Phase = iota
	RedecePhase
	AllDone
)

const (
	Working State = iota
	Waitting
	Done
)

type Task struct {
	TaskType   TaskType
	TaskId     int
	ReducerNum int
	FileSlice  []string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
