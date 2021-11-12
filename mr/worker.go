package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// KeyValue struct
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker function
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	id := 1
	reply := RequestTask(id)
	switch reply.TaskType {
	case MAP:
		content, _ := readInputFile(reply.FileName)
		kvaSet := mapf(reply.FileName, content)
		writeToIntermediateFile(1, reply.NReduce, kvaSet)
		Notify(id, COMPLETED)
		id++
	case EXIT:
		return
	}
}
func readInputFile(fileName string) (string, error) {
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		return "", err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

// ByKey type for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// intermediate files is mr-X-Y, where X is the Map task number, and Y is the reduce task number
func writeToIntermediateFile(x, nReduce int, kvaSet []KeyValue) {
	sort.Sort(ByKey(kvaSet))
	hashMap := make(map[int][]KeyValue)
	for _, kva := range kvaSet {
		y := ihash(kva.Key) % nReduce
		hashMap[y] = append(hashMap[y], kva)
	}

	for y, kvaSet := range hashMap {
		oname := fmt.Sprintf("mr-%d-%d", x, y)

		ofile, _ := os.CreateTemp("", oname+"-*")

		w := bufio.NewWriter(ofile)

		sort.Sort(ByKey(kvaSet))
		for _, kva := range kvaSet {
			fmt.Fprintf(w, "%v %v\n", kva.Key, kva.Value)
		}
		w.Flush()
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}
}

// RequestTask func
func RequestTask(workerID int) RequestTaskReply {
	args := RequestTaskArgs{workerID, IDLE}
	reply := RequestTaskReply{}
	call("Coordinator.RequestTask", &args, &reply)
	return reply
}

// Notify func
func Notify(workerID int, status state) {
	args := RequestTaskArgs{workerID, status}
	call("Coordinator.Notify", &args, nil)
}

// CallExample func
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
