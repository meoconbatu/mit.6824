package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
	done := false
	for !done {
		workerID := int(uuid.New().ID())
		reply, ok := RequestTask(workerID)
		if !ok {
			return
		}
		switch reply.TaskType {
		case MAP:
			content, _ := readInputFile(reply.FileName)
			kvaSet := mapf(reply.FileName, content)
			writeToIntermediateFile(workerID, reply.NReduce, kvaSet)
			ok = Notify(workerID, COMPLETED)
		case REDUCE:
			intermediate, _ := readIntermediateFile(reply.FileName)
			writeToOutputFile(reply.FileName, intermediate, reducef)
			ok = Notify(workerID, COMPLETED)
		case WAITING:
			time.Sleep(time.Millisecond * 50)
		case EXIT:
			return
		}
		if !ok {
			done = true
		}
	}
}
func readInputFile(fileName string) (string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return "", err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func readIntermediateFile(fileName string) ([]KeyValue, error) {
	matches, err := filepath.Glob(fileName)
	if err != nil {
		return nil, err
	}
	kva := make([]KeyValue, 0)
	for _, fileName := range matches {
		file, err := os.Open(fileName)
		if err != nil {
			return nil, err
		}

		scanner := bufio.NewScanner(file)

		var k, v string
		for scanner.Scan() {
			_, err = fmt.Sscanf(scanner.Text(), "%s %s", &k, &v)
			if err != nil {
				return nil, err
			}
			kva = append(kva, KeyValue{k, v})
		}

		file.Close()
	}
	return kva, nil
}

// ByKey type for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// intermediate files is mr-X-Y, where X is the Map task number, and Y is the reduce task number
func writeToIntermediateFile(x, nReduce int, kvaSet []KeyValue) {
	hashMap := make(map[int][]KeyValue)
	for _, kva := range kvaSet {
		y := ihash(kva.Key) % nReduce
		hashMap[y] = append(hashMap[y], kva)
	}

	for y, kvaSet := range hashMap {
		oname := fmt.Sprintf("mr-%d-%d", x, y)
		ofile, _ := ioutil.TempFile("", oname+"-*")

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

// output files is mr-out-Y, and Y is the reduce task number
func writeToOutputFile(fileName string, intermediate []KeyValue, reducef func(string, []string) string) {
	oname := strings.Replace(fileName, "*", "out", -1)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	w := bufio.NewWriter(ofile)

	sort.Sort(ByKey(intermediate))

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

		fmt.Fprintf(w, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	w.Flush()
}

// RequestTask func
func RequestTask(workerID int) (RequestTaskReply, bool) {
	args := RequestTaskArgs{workerID, IDLE}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	return reply, ok
}

// Notify func
func Notify(workerID int, status state) bool {
	args := RequestTaskArgs{workerID, status}
	return call("Coordinator.Notify", &args, nil)
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
	var wg sync.WaitGroup
	wg.Wait()
	fmt.Println(err)
	return false
}
