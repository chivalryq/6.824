package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
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

var workerID int

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// now we can use PID to identify worker because all workers are on the same machine
	workerID = os.Getpid()

	// Your worker implementation here.
	for true {
		reply := TryGetJob()
		if reply.Done {
			fmt.Println("Done all job, exit")
			break
		}
		switch reply.JobType {
		case MapType:
			filename := reply.Map.File
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			_ = file.Close()
			kva := mapf(filename, string(content))
			kvmap := map[int][]KeyValue{}
			for _, kv := range kva {
				reduceKey := ihash(kv.Key) % reply.NReduce
				kvmap[reduceKey] = append(kvmap[reduceKey], kv)
			}
			SaveKVMapIntermediate(reply.Map.TaskNum, kvmap)
			ReportMapDone(reply.Map.TaskNum)
		case ReduceType:
			reducePiece:=reply.Reduce.ReducePiece
			files, err := filepath.Glob("mr-*-" + strconv.Itoa(reducePiece))
			if err != nil {
				fmt.Printf("Fail to glob files for reduce task num %d", reducePiece)
			}
			intermediate := ReadIntermediate(files)
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", reducePiece)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			_ = ofile.Close()
			ReportReduceDone(reducePiece)
		case WaitType:
			time.Sleep(1 * time.Second)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

func ReadIntermediate(files []string) []KeyValue {
	var kva []KeyValue
	for _, f := range files {
		file, err := os.Open(f)
		if err != nil {
			fmt.Printf("Fail to open file %s\n", file)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva
}

func SaveKVMapIntermediate(taskNum int, kvm map[int][]KeyValue) {
	for reducePiece, kva := range kvm {
		interFilename := fmt.Sprintf("mr-%d-%d", taskNum, reducePiece)
		file, err := os.OpenFile(interFilename, os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			fmt.Printf("Fail to open file %s: %v\n", interFilename, err)
		}
		enc := json.NewEncoder(file)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Printf("Fail to encode kv in file %s: %v\n", interFilename, err)
				return
			}
		}
	}
}

func TryGetJob() GetJobReply {
	args := GetJobArgs{WorkerID: workerID}
	reply := GetJobReply{}

	ok := call("Coordinator.RetrieveJob", &args, &reply)
	if ok {
		switch reply.JobType {
		case MapType:
			fmt.Printf("reply TaskNum=%d File=%s\n", reply.Map.TaskNum, reply.Map.File)
		case ReduceType:
			fmt.Printf("reply ReducePiece=%d\n", reply.Reduce.ReducePiece)
		case WaitType:
			fmt.Printf("wait for 1 second\n")
		}
	} else {
		fmt.Printf("Done all job, master exited, exit")
		os.Exit(0)
	}
	return reply
}

func ReportMapDone(MapNum int) {
	args := ReportArgs{
		JobType: MapType,
		Map: Map{
			TaskNum: MapNum,
		},
	}
	reply := ReportReply{}
	ok := call("Coordinator.Report", &args, &reply)
	if ok {
		fmt.Printf("Report map %d done\n", MapNum)
	} else {
		fmt.Printf("call failed\n")
	}
}

func ReportReduceDone(ReducePiece int){
	args := ReportArgs{
		JobType: ReduceType,
		Reduce: Reduce{
			ReducePiece: ReducePiece,
		},
	}
	reply := ReportReply{}
	ok := call("Coordinator.Report", &args, &reply)
	if ok {
		fmt.Printf("Report reduce %d done\n", ReducePiece)
	} else {
		fmt.Printf("call failed\n")
	}
}

//
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
