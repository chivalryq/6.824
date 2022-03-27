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

// Add your RPC definitions here.

type GetJobArgs struct {
	WorkerID int
}

type JType string

const MapType JType = "Map"
const ReduceType JType = "Reduce"

// WaitType means worker should wait till all Map jobs are done
const WaitType JType = "wait"

type Map struct {
	File    string
	TaskNum int
}

type Reduce struct {
	ReducePiece int
}

type GetJobReply struct {
	Done    bool
	JobType JType
	Map     Map
	Reduce  Reduce
	NReduce int
}

type ReportArgs struct {
	JobType JType
	Map     Map
	Reduce  Reduce
}

type ReportReply struct {
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
