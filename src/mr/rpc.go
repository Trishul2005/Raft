package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

type TaskType string 
const (
	MapTask    TaskType = "MAP"
	ReduceTask TaskType = "REDUCE"
	WaitTask   TaskType = "WAIT"
	ExitTask   TaskType = "EXIT"
)

// worker request task from coordinator
type TaskRequest struct {

}

// coordinator reply task to worker
type TaskReply struct {
	Type TaskType
	TaskId int
	FileName string
	NReduce int
	NMap int
}
// MapTask use: Type, TaskId, FileName, NReduce
// ReduceTask use: Type, TaskId, NMap
// WaitTask use: Type
// ExitTask use: Type

// worker report task complete to coordinator
type TaskCompleteRequest struct {
	Type TaskType
	TaskId int
}

// coordinator reply task complete to worker
type TaskCompleteReply struct {

}


