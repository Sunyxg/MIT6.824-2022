package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
// Map 函数返回键值对切片的形式
type KeyValue struct {
	Key   string
	Value string
}

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

	// Your worker implementation here.
	for {
		reps := DoHeartbeat()
		switch reps.Jobtype {
		case 0:
			DoMapTask(mapf, &reps)
		case 1:
			time.Sleep(1 * time.Second)
		case 2:
			DoReduceTask(reducef, &reps)
		case 3:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", reps.Jobtype))
		}

	}

}

func DoHeartbeat() HeartbeatResponse {
	req := GetRequest{}
	reps := HeartbeatResponse{}
	// send the RPC request, wait for the reply.
	ok := call("Coordinator.Heartbeat", &req, &reps)
	if !ok {
		fmt.Println("Call Heartbeat failed!")
	}
	return reps
}

func FinishTaskReply(MoR int, taskid int) {
	req := TaskDoneRequest{}
	req.Taskid = taskid
	req.MorR = MoR
	reps := TaskDoneResponse{}
	// send the RPC request, wait for the reply.
	ok := call("Coordinator.Taskover", &req, &reps)
	if !ok {
		fmt.Println("Call Taskover failed!")
	}
}

func DoMapTask(mapf func(string, string) []KeyValue, reps *HeartbeatResponse) {
	filename := reps.FileName
	taskid := reps.TaskId
	nReduce := reps.NReduce

	// fmt.Printf("Map: %v\n", reps.TaskId)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	kva := mapf(filename, string(content))
	intermediate := make([][]KeyValue, nReduce)

	for _, kv := range kva {
		ReduceId := ihash(kv.Key) % nReduce
		intermediate[ReduceId] = append(intermediate[ReduceId], kv)
	}

	for i, kvs := range intermediate {
		writeToLocalFile(taskid, i, kvs)
	}

	FinishTaskReply(0, taskid)
	// fmt.Printf("Map: %v Finish!!!!\n", reps.TaskId)
}

func DoReduceTask(reducef func(string, []string) string, reps *HeartbeatResponse) {
	taskid := reps.TaskId
	nmap := reps.NMap
	intermediate := []KeyValue{}
	// fmt.Printf("Reduce: %v\n", reps.TaskId)

	for i := 0; i < nmap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, taskid)
		//fmt.Println(filename)
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	tmpfile, err := ioutil.TempFile(os.TempDir(), "rm-tmp")
	if err != nil {
		fmt.Printf("Create tempFile failed!\n")
	}
	defer func() {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}()

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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	oname := fmt.Sprintf("mr-out-%d", taskid)
	err = os.Rename(tmpfile.Name(), oname)
	if err != nil {
		fmt.Printf("Rename tempFile failed!\n")
	}

	FinishTaskReply(1, taskid)
	// fmt.Printf("Reduce: %v Finish!!!!\n", reps.TaskId)
}

func writeToLocalFile(m int, r int, kvs []KeyValue) {
	tmpfile, err := ioutil.TempFile(os.TempDir(), "rm-tmp")
	if err != nil {
		fmt.Printf("Create tempFile failed!\n")
	}
	defer func() {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}()

	enc := json.NewEncoder(tmpfile)
	for _, kv := range kvs {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Json encode failed: %v", err)
		}
	}
	oname := fmt.Sprintf("mr-%d-%d", m, r)
	err = os.Rename(tmpfile.Name(), oname)
	if err != nil {
		fmt.Printf("Rename tempFile failed!\n")
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
