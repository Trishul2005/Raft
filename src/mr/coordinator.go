package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type status string

const (
	Idle       status = "IDLE"
	InProgress status = "IN_PROGRESS"
	Done       status = "DONE"
)

type phase string

const (
	MapPhase       phase = "MAP"
	ReducePhase    phase = "REDUCE"
	CompletedPhase phase = "COMPLETED"
)

type Coordinator struct {
	files           []string
	nReduce         int
	mapTasks        []status
	reduceTasks     []status
	mapStartTime    []time.Time
	reduceStartTime []time.Time
	phase           phase
	mu              sync.Mutex
}

/*
	called by worker to request a task from the coordinator
*/
func (c *Coordinator) RequestTask(args *TaskRequest, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// if the entire job is completed, instruct the worker to exit
	if c.phase == CompletedPhase {
		reply.Type = ExitTask
		return nil
	}

	// assign tasks based on the current phase of the job
	if c.phase == MapPhase {
		if c.tryAssignMapTask(reply) {
			return nil
		}
		if c.allMapDone() {
			c.phase = ReducePhase
		}
		reply.Type = WaitTask
		return nil
	}

	if c.phase == ReducePhase {
		if c.tryAssignReduceTask(reply) {
			return nil
		}
		if c.allReduceDone() {
			c.phase = CompletedPhase
			reply.Type = ExitTask
			return nil
		}
		reply.Type = WaitTask
		return nil
	}

	reply.Type = WaitTask
	return nil
}

/*
	called by a worker to report that it has completed a task.
	updates the status of the task to Done if it was in progress.
*/
func (c *Coordinator) ReportTaskComplete(args *TaskCompleteRequest, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == MapTask {
		if c.mapTasks[args.TaskId] == InProgress {
			c.mapTasks[args.TaskId] = Done
		}
	} else if args.Type == ReduceTask {
		if c.reduceTasks[args.TaskId] == InProgress {
			c.reduceTasks[args.TaskId] = Done
		}
	}
	return nil
}

// ================ HELPERS FOR ASSIGNING TASKS AND CHECKING STATUS ================

/*
	assignTask tries to assign a task to a worker. 
	It checks for idle tasks and assigns the first one it finds.
*/

func (c *Coordinator) tryAssignMapTask(reply *TaskReply) bool {
	for taskId := 0; taskId < len(c.mapTasks); taskId++ {
		if c.mapTasks[taskId] == Idle {
			c.mapTasks[taskId] = InProgress
			c.mapStartTime[taskId] = time.Now()

			reply.Type = MapTask
			reply.TaskId = taskId
			reply.FileName = c.files[taskId]
			reply.NReduce = c.nReduce
			return true
		}
	}
	return false
}

func (c *Coordinator) tryAssignReduceTask(reply *TaskReply) bool {
	for taskId := 0; taskId < len(c.reduceTasks); taskId++ {
		if c.reduceTasks[taskId] == Idle {
			c.reduceTasks[taskId] = InProgress
			c.reduceStartTime[taskId] = time.Now()

			reply.Type = ReduceTask
			reply.TaskId = taskId
			reply.NMap = len(c.files)
			return true
		}
	}
	return false
}

/*
	allDone checks if all tasks of a given type (map or reduce) are completed.
*/

func (c *Coordinator) allMapDone() bool {
	for _, s := range c.mapTasks {
		if s != Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) allReduceDone() bool {
	for _, s := range c.reduceTasks {
		if s != Done {
			return false
		}
	}
	return true
}
// ================ END OF HELPERS ================

/*
	taskTimeoutWatcher periodically checks for tasks that have been in progress
	for too long (>10 seconds) and resets them to idle so they can be reassigned.
*/
func (c *Coordinator) taskTimeoutWatcher() {
	for {
		time.Sleep(1 * time.Second)
		c.mu.Lock()
		if c.phase == CompletedPhase {
			c.mu.Unlock()
			return
		}
		now := time.Now()
		for taskId, status := range c.mapTasks {
			if status == InProgress && now.Sub(c.mapStartTime[taskId]) > 10*time.Second {
				log.Printf("*re*-starting map %s %d", c.files[taskId], taskId)
				c.mapTasks[taskId] = Idle
			}
		}
		for taskId, status := range c.reduceTasks {
			if status == InProgress && now.Sub(c.reduceStartTime[taskId]) > 10*time.Second {
				log.Printf("*re*-starting reduce %d", taskId)
				c.reduceTasks[taskId] = Idle
			}
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.phase == CompletedPhase
}

/*
	initializes the coordinator and struct values
	it also starts a goroutine to watch for task timeouts and starts the RPC server.
*/
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:           files,
		nReduce:         nReduce,
		mapTasks:        make([]status, len(files)),
		reduceTasks:     make([]status, nReduce),
		mapStartTime:    make([]time.Time, len(files)),
		reduceStartTime: make([]time.Time, nReduce),
		phase:           MapPhase,
	}

	for i := range c.mapTasks {
		c.mapTasks[i] = Idle
	}
	for i := range c.reduceTasks {
		c.reduceTasks[i] = Idle
	}

	go c.taskTimeoutWatcher()
	c.server(sockname)
	return &c
}
