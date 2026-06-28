package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "time"
import "sort"


// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		args := TaskRequest{}
		reply := TaskReply{}

		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			log.Printf("%d: call failed", os.Getpid())
			return
		}

		switch reply.Type {

		case MapTask:
			doMap(reply, mapf)
		
		case ReduceTask:
			doReduce(reply, reducef)

		case WaitTask:
			time.Sleep(500 * time.Millisecond)

		case ExitTask:
			log.Printf("%d: exiting", os.Getpid())
			return

		default:
			log.Printf("%d: unknown task type %v", os.Getpid(), reply.Type)
			return

		}

	}

}

func doMap(reply TaskReply, mapf func(string, string) []KeyValue) {

	// read the input file
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("%d: cannot open %v", os.Getpid(), reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("%d: cannot read %v", os.Getpid(), reply.FileName)
	}
	file.Close()

	// apply the map function
	kva := mapf(reply.FileName, string(content))

	// partition the intermediate key-value pairs into nReduce files
	partitions := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % reply.NReduce
		partitions[reduceTaskNum] = append(partitions[reduceTaskNum], kv)
	}

	// write each partition to a separate file atomically
	for i, partition := range partitions {
		tmpFile, err := os.CreateTemp("", "mr-map-tmp-*")
		if err != nil {
			log.Fatalf("%d: cannot create temp file: %v", os.Getpid(), err)
		}
		enc := json.NewEncoder(tmpFile)
		for _, kv := range partition {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("%d: cannot encode %v", os.Getpid(), kv)
			}
		}
		tmpFile.Close()
		outName := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
		if err := os.Rename(tmpFile.Name(), outName); err != nil {
			log.Fatalf("%d: cannot rename to %v: %v", os.Getpid(), outName, err)
		}
	}

	// report task completion to the coordinator
	args := TaskCompleteRequest{Type: MapTask, TaskId: reply.TaskId}
	replyComplete := TaskCompleteReply{}
	ok := call("Coordinator.ReportTaskComplete", &args, &replyComplete)
	if !ok {
		log.Printf("%d: call failed", os.Getpid())
	}
}

func doReduce(reply TaskReply, reducef func(string, []string) string) {

	// read all intermediate files for this reduce task
	intermediate := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, reply.TaskId)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("%d: cannot open %v", os.Getpid(), fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// sort the intermediate key-value pairs by key
	sort.Sort(ByKey(intermediate))

	// create the output file
	outName := fmt.Sprintf("mr-out-%d", reply.TaskId)
	tmpFile, err := os.CreateTemp("", "mr-reduce-tmp-*")
	if err != nil {
		log.Fatalf("%d: cannot create temp file: %v", os.Getpid(), err)
	}

	// apply the reduce function to each key and write to the output file
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tmpFile.Close()

	if err := os.Rename(tmpFile.Name(), outName); err != nil {
		log.Fatalf("%d: cannot rename to %v: %v", os.Getpid(), outName, err)
	}

	// report task completion to the coordinator
	args := TaskCompleteRequest{Type: ReduceTask, TaskId: reply.TaskId}
	replyComplete := TaskCompleteReply{}
	ok := call("Coordinator.ReportTaskComplete", &args, &replyComplete)
	if !ok {
		log.Printf("%d: call failed", os.Getpid())
	}
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
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Printf("dialing: %v", err)
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
